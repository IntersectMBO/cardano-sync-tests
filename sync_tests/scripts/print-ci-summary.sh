#! /usr/bin/env bash
# shellcheck shell=bash
# print-ci-summary.sh [CI_STEP_LOG]
#
# Print pytest PASSED/SKIPPED/FAILED lines and final summary from ci_step.log.

set -uo pipefail

LOG="${1:-test_workdir/ci_step.log}"

echo "::group::Test summary"
if [ ! -f "$LOG" ]; then
  echo "[ci_step.log not found]"
  echo "::endgroup::"
  exit 0
fi

printed="$(
  awk '
    / (PASSED|SKIPPED|FAILED) \[[[:space:]]*[0-9]+%\]/ { started=1 }
    /^[[:space:]]*(PASSED|SKIPPED|FAILED)[[:space:]]+\[[[:space:]]*[0-9]+%\]/ { started=1 }
    /::.* (PASSED|SKIPPED|FAILED) \[/ { started=1 }
    /^=+ short test summary info =+/ { started=1 }
    started { print; found=1 }
    /^=+[[:space:]]+[0-9]+ passed/ { print; exit }
    /^=+[[:space:]]+[0-9]+ failed/ { print; exit }
    /^=+.* (passed|failed|error).* in [0-9]/ { if (started) print; exit }
    END { if (!found) exit 1 }
  ' "$LOG" 2>/dev/null || true
)"

if [ -n "$printed" ]; then
  printf '%s\n' "$printed"
else
  echo "[pytest summary not found; last 80 lines of ci_step.log:]"
  tail -n 80 "$LOG"
fi
echo "::endgroup::"
