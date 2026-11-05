#!/usr/bin/env bash
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

scripts_dir="$(cd "$(dirname "$0")" && pwd)"

# The report needs the workdir, the network and whether db-sync ran. They are
# read from the pytest options rather than asked for twice.
workdir="test_workdir"
environment="preview"
kind="node"
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
  case "${args[i]}" in
    --workdir) workdir="${args[i + 1]:-$workdir}" ;;
    --workdir=*) workdir="${args[i]#--workdir=}" ;;
    --environment) environment="${args[i + 1]:-$environment}" ;;
    --environment=*) environment="${args[i]#--environment=}" ;;
    --db-sync-revision | --db-sync-revision=*) kind="dbsync" ;;
  esac
done

mkdir -p "$workdir"
# pytest writes the file only at session end; an old one must not be reported.
rm -f "$workdir/junit.xml"
pytest_exit=0
pytest "$@" --junitxml "$workdir/junit.xml" || pytest_exit=$?

bash "$scripts_dir/report_stats.sh" "$workdir" "$pytest_exit" "$kind" "$environment" || :
exit "$pytest_exit"
