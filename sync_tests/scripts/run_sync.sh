#! /usr/bin/env bash
# shellcheck shell=bash
# run_sync.sh <pytest arguments...>
#
# Run the sync tests, then report the run to the tcache. One entry point for
# CI and a laptop: pytest gets every argument as given, plus a JUnit file in
# the workdir for the report. The exit code is pytest's; the report never
# changes it.

set -uo pipefail

if [ "$#" -eq 0 ]; then
  echo "Usage: $0 <pytest arguments...>" >&2
  exit 1
fi

SCRIPTS_DIR="$(cd "$(dirname "$0")" && pwd)"

# The report needs the workdir, the network and whether db-sync ran. They are
# read from the pytest options rather than asked for twice.
WORKDIR="test_workdir"
ENVIRONMENT="preview"
KIND="node"
ARGS=("$@")
for ((i = 0; i < ${#ARGS[@]}; i++)); do
  case "${ARGS[i]}" in
    --workdir) WORKDIR="${ARGS[i + 1]:-$WORKDIR}" ;;
    --workdir=*) WORKDIR="${ARGS[i]#--workdir=}" ;;
    --environment) ENVIRONMENT="${ARGS[i + 1]:-$ENVIRONMENT}" ;;
    --environment=*) ENVIRONMENT="${ARGS[i]#--environment=}" ;;
    --db-sync-revision | --db-sync-revision=*) KIND="dbsync" ;;
  esac
done

mkdir -p "$WORKDIR"
# pytest writes the file only at session end; an old one must not be reported.
rm -f "$WORKDIR/junit.xml"
PYTEST_EXIT=0
pytest "$@" --junitxml "$WORKDIR/junit.xml" || PYTEST_EXIT=$?

bash "$SCRIPTS_DIR/report_stats.sh" "$WORKDIR" "$PYTEST_EXIT" "$KIND" "$ENVIRONMENT" || :
exit "$PYTEST_EXIT"
