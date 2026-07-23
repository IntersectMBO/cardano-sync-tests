#! /usr/bin/env bash
# shellcheck shell=bash
# print-ci-summary.sh [CI_STEP_LOG] [EXIT_CODE]
#
# Print pytest PASSED/SKIPPED/FAILED lines and final summary from ci_step.log.
# When running in GitHub Actions (GITHUB_STEP_SUMMARY set), also write the
# same summary to the job's Summary tab so results are visible at a glance,
# without opening the raw log and expanding the folded group.

set -uo pipefail

LOG="${1:-test_workdir/ci_step.log}"
EXIT_CODE="${2:-}"

if [ ! -f "$LOG" ]; then
  echo "::group::Test summary"
  echo "[ci_step.log not found]"
  echo "::endgroup::"
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    {
      echo "## Sync test summary"
      echo ""
      echo "ci_step.log not found."
    } >> "$GITHUB_STEP_SUMMARY" || true
  fi
  exit 0
fi

printed="$(
  awk '
    / (PASSED|SKIPPED|FAILED) \[[[:space:]]*[0-9]+%\]/ { started=1 }
    /^[[:space:]]*(PASSED|SKIPPED|FAILED)[[:space:]]+\[[[:space:]]*[0-9]+%\]/ { started=1 }
    /::.* (PASSED|SKIPPED|FAILED) \[/ { started=1 }
    /^=+ short test summary info =+/ { started=1 }
    started { print; found=1 }
    /^=+[[:space:]]+[0-9]+ passed/ { if (!started) print; exit }
    /^=+[[:space:]]+[0-9]+ failed/ { if (!started) print; exit }
    /^=+.* (passed|failed|error).* in [0-9]/ { if (!started) print; exit }
    END { if (!found) exit 1 }
  ' "$LOG" 2>/dev/null || true
)"

echo "::group::Test summary"
if [ -n "$printed" ]; then
  printf '%s\n' "$printed"
else
  echo "[pytest summary not found; last 80 lines of ci_step.log:]"
  tail -n 80 "$LOG"
fi
echo "::endgroup::"

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  status_line="Result: status unknown"
  if [ "$EXIT_CODE" = "0" ]; then
    status_line="Result: :white_check_mark: passed"
  elif [ -n "$EXIT_CODE" ]; then
    status_line="Result: :x: failed (exit $EXIT_CODE)"
  fi

  {
    echo "## Sync test summary"
    echo ""
    echo "$status_line"
    echo ""
    echo '```'
    if [ -n "$printed" ]; then
      printf '%s\n' "$printed"
    else
      echo "pytest summary not found; see the job log for details."
    fi
    echo '```'
  } >> "$GITHUB_STEP_SUMMARY" || true
fi

# This script is purely informational (console log + job summary); a glitch
# writing either one must never fail the calling CI step, which runs under
# set -euo pipefail and would otherwise abort before it reports pytest's own
# exit code.
exit 0
