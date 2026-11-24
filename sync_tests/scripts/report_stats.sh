#! /usr/bin/env bash
# shellcheck shell=bash
# report_stats.sh <workdir> <pytest_exit_code> <node|dbsync> <environment>
#
# Upload the run statistics of one sync run to the tcache. Best effort: call
# it with `|| :` so a failure here cannot change the caller's exit code.
# Does nothing unless ENABLE_TCACHE_STATS is on and the two secrets are set.

set -uo pipefail

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <workdir> <pytest_exit_code> <node|dbsync> <environment>" >&2
  exit 1
fi

WORKDIR="$1"
EXIT_CODE="$2"
KIND="$3"
ENVIRONMENT="$4"
SCRIPTS_DIR="$(cd "$(dirname "$0")" && pwd)"

summary() {
  echo "$1"
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    echo "tcache stats: $1" >> "$GITHUB_STEP_SUMMARY" || true
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

JUNIT="$WORKDIR/junit.xml"
if [ ! -f "$JUNIT" ]; then
  summary "failed, no $JUNIT"
  exit 1
fi

# The node-tests secret ends in `/results`; accept that form and the root.
ROOT="${TCACHE_URL%/}"
STATS_URL="${ROOT%/results}/stats"

STATS_JSON="$(mktemp)" || exit 1
RESPONSE="$(mktemp)" || exit 1
CURL_ERR="$(mktemp)" || exit 1
# shellcheck disable=SC2064
trap "rm -f '$STATS_JSON' '$RESPONSE' '$CURL_ERR'" EXIT

if ! python3 "$SCRIPTS_DIR/stats_json.py" --workdir "$WORKDIR" --junit "$JUNIT" \
    --exit-code "$EXIT_CODE" --kind "$KIND" --environment "$ENVIRONMENT" \
    --output "$STATS_JSON"; then
  summary "failed, could not build the stats document"
  exit 1
fi

echo "Reporting sync run stats to the tcache"
if ! curl -sS -X PUT --fail-with-body --connect-timeout 10 --max-time 60 \
    -u "$TCACHE_BASIC_AUTH" "$STATS_URL" \
    -H "Content-Type: application/json" --data-binary "@$STATS_JSON" \
    -o "$RESPONSE" 2> "$CURL_ERR"; then
  summary "failed, upload rejected: $(cat "$CURL_ERR" "$RESPONSE" | head -c 300)"
  exit 1
fi

# A proxy with no route for /stats answers 200 with an empty body. Only an
# answer that names our run proves the service stored it.
RUN_ID="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["run_id"])' "$STATS_JSON")"
if ! python3 -c 'import json,sys; sys.exit(json.load(open(sys.argv[1])).get("run_id") != sys.argv[2])' \
    "$RESPONSE" "$RUN_ID" 2>/dev/null; then
  summary "failed, no confirmation from the tcache for run $RUN_ID"
  exit 1
fi

summary "stored run $RUN_ID"
