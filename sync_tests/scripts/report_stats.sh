#!/usr/bin/env bash
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

workdir="$1"
exit_code="$2"
kind="$3"
environment="$4"
scripts_dir="$(cd "$(dirname "$0")" && pwd)"

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

junit="$workdir/junit.xml"
if [ ! -f "$junit" ]; then
  summary "failed, no $junit"
  exit 1
fi

# The node-tests secret ends in `/results`; accept that form and the root.
root="${TCACHE_URL%/}"
stats_url="${root%/results}/stats"

stats_json="$(mktemp)" || exit 1
response="$(mktemp)" || exit 1
curl_err="$(mktemp)" || exit 1
# shellcheck disable=SC2064
trap "rm -f '$stats_json' '$response' '$curl_err'" EXIT

if ! python3 "$scripts_dir/stats_json.py" --workdir "$workdir" --junit "$junit" \
    --exit-code "$exit_code" --kind "$kind" --environment "$environment" \
    --output "$stats_json"; then
  summary "failed, could not build the stats document"
  exit 1
fi

echo "Reporting sync run stats to the tcache"
if ! curl -sS -X PUT --fail-with-body --connect-timeout 10 --max-time 60 \
    -u "$TCACHE_BASIC_AUTH" "$stats_url" \
    -H "Content-Type: application/json" --data-binary "@$stats_json" \
    -o "$response" 2> "$curl_err"; then
  summary "failed, upload rejected: $(cat "$curl_err" "$response" | head -c 300)"
  exit 1
fi

# A proxy with no route for /stats answers 200 with an empty body. Only an
# answer that names our run proves the service stored it.
run_id="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["run_id"])' "$stats_json")"
if ! python3 -c 'import json,sys; sys.exit(json.load(open(sys.argv[1])).get("run_id") != sys.argv[2])' \
    "$response" "$run_id" 2>/dev/null; then
  summary "failed, no confirmation from the tcache for run $run_id"
  exit 1
fi

summary "stored run $run_id"
