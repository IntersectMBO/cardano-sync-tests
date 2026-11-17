#! /usr/bin/env bash
# shellcheck shell=bash
# report_sync_results.sh <workdir> <environment>
#
# Upload the results of one node sync run to the tcache, where the sync
# analyzer reads them. Best effort: call it with `|| :` so a failure here
# cannot change the caller's exit code. Does nothing unless
# ENABLE_TCACHE_STATS is on and the two secrets are set, the same switch as
# the run statistics.

set -uo pipefail

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <workdir> <environment>" >&2
  exit 1
fi

WORKDIR="$1"
ENVIRONMENT="$2"
SCRIPTS_DIR="$(cd "$(dirname "$0")" && pwd)"

summary() {
  echo "$1"
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    echo "tcache sync results: $1" >> "$GITHUB_STEP_SUMMARY" || true
  fi
}

case "${ENABLE_TCACHE_STATS:-}" in
  1 | [Tt][Rr][Uu][Ee] | [Yy] | [Yy][Ee][Ss]) ;;
  *)
    summary "disabled, ENABLE_TCACHE_STATS is not on"
    exit 0
    ;;
esac

if [ -z "${TCACHE_BASIC_AUTH:-}" ] || [ -z "${TCACHE_URL:-}" ]; then
  summary "disabled, TCACHE_BASIC_AUTH or TCACHE_URL is not set"
  exit 0
fi

RESULTS="$WORKDIR/node_sync_results.json"
if [ ! -f "$RESULTS" ]; then
  summary "failed, no $RESULTS"
  exit 1
fi
# The runner marks when the run started. Results older than that are from
# an earlier run in the same workdir, which a sync that fails early leaves.
if [ -f "$WORKDIR/.run_started" ] && [ ! "$RESULTS" -nt "$WORKDIR/.run_started" ]; then
  summary "failed, $RESULTS is from an earlier run"
  exit 1
fi

# The node-tests secret ends in `/results`; accept that form and the root.
ROOT="${TCACHE_URL%/}"
ROOT="${ROOT%/results}"

ZIP="$(mktemp)" || exit 1
NAME="$(mktemp)" || exit 1
RESPONSE="$(mktemp)" || exit 1
CURL_ERR="$(mktemp)" || exit 1
# shellcheck disable=SC2064
trap "rm -f '$ZIP' '$NAME' '$RESPONSE' '$CURL_ERR'" EXIT

BUNDLE_EXIT=0
python3 "$SCRIPTS_DIR/sync_results_zip.py" --workdir "$WORKDIR" --environment "$ENVIRONMENT" \
  --output "$ZIP" > "$NAME" || BUNDLE_EXIT=$?
if [ "$BUNDLE_EXIT" -eq 3 ]; then
  summary "skipped, the results name no node version"
  exit 0
fi
if [ "$BUNDLE_EXIT" -ne 0 ]; then
  summary "failed, could not bundle the results"
  exit 1
fi
IFS=$'\t' read -r VERSION STARTED < "$NAME"
# The version names the run in the URL path. A branch name holds a slash,
# which the path cannot; the same replacement the CI artifact name uses.
VERSION="${VERSION//[^A-Za-z0-9._-]/-}"

echo "Reporting sync results of $VERSION on $ENVIRONMENT to the tcache"
# The service answers 503 while it is busy; this zip is the only copy of the
# results outside the CI artifact, so the upload is retried.
if ! curl -sS -X PUT --fail-with-body --connect-timeout 10 --max-time 300 \
    --retry 3 --retry-delay 5 \
    -u "$TCACHE_BASIC_AUTH" "$ROOT/sync-results/$VERSION" \
    -F "syncresults=@$ZIP;filename=sync_results.zip" \
    -F "environment=$ENVIRONMENT" -F "started=$STARTED" \
    -o "$RESPONSE" 2> "$CURL_ERR"; then
  summary "failed, upload rejected: $(cat "$CURL_ERR" "$RESPONSE" | head -c 300)"
  exit 1
fi

# A proxy with no route answers 200 with an empty body. Only an answer that
# names our run proves the service stored it.
RUN_ID="$(python3 -c 'import json,sys
try:
    doc = json.load(open(sys.argv[1]))
except ValueError:
    sys.exit(1)
if doc.get("sync_results") != sys.argv[2]:
    sys.exit(1)
print(doc["id"])' "$RESPONSE" "$VERSION" 2>/dev/null)"
if [ -z "$RUN_ID" ]; then
  summary "failed, no confirmation from the tcache for $VERSION"
  exit 1
fi

summary "stored $VERSION on $ENVIRONMENT as run $RUN_ID"
