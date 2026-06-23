#! /usr/bin/env bash
# shellcheck shell=bash
# monitor-start.sh <WORK_DIR> <ENV_NAME>
#
# Long-lived background monitor for the full duration of a sync run.
# Logs system memory, disk, and process RSS every 5 minutes.
# On exit, scans the kernel journal for OOM evidence.
# Tool stderr (missing packages, permission errors, etc.) is appended to
# monitor_stderr.log in the work dir so CI artifacts show the real failure.
#
# Usage:
#   bash monitor-start.sh /path/to/test_workdir mainnet

# Do NOT use set -e here — this is a long-running monitor loop.
# Individual command failures must never kill the monitor.
set -uo pipefail

WORK_DIR="${1:?"Need path to work dir"}"
ENV_NAME="${2:?"Need environment name"}"
WORK_DIR="$(readlink -m "$WORK_DIR")"

MONITOR_LOG="$WORK_DIR/monitor.log"
OOM_LOG="$WORK_DIR/oom.log"
MONITOR_ERR="$WORK_DIR/monitor_stderr.log"

# Tool failures go here (not discarded). Parent may also attach Popen stderr to the same file.
: > "$MONITOR_ERR"
exec 2>>"$MONITOR_ERR"

# Linux task comm is at most 15 characters; "cardano-db-sync" truncates to "cardano-db-sy".
_rss_sum_kb() {
  local comm="$1"
  ps -C "$comm" -o rss= | awk '{s+=$1} END {print s+0}'
}

_log_resource_snapshot() {
  local title="$1"
  {
    echo "===== $title ====="
    echo "--- Memory ---"
    free -h || echo "  (free unavailable)"
    echo "--- Disk ---"
    df -h "$WORK_DIR" || echo "  (df unavailable)"
    echo "--- Disk usage (working dir tree) ---"
    if command -v du >/dev/null 2>&1; then
      du -sh "$WORK_DIR" 2>&1 || du -sk "$WORK_DIR" 2>&1 || echo "  (du failed)"
    else
      echo "  (du unavailable)"
    fi
    echo "--- Process RSS (kB) ---"
    local proc rss
    for proc in postgres cardano-db-sync cardano-node; do
      rss="$(_rss_sum_kb "$proc")"
      if [ "${rss:-0}" -eq 0 ] && [ "$proc" = "cardano-db-sync" ]; then
        rss="$(_rss_sum_kb "cardano-db-sy")"
      fi
      echo "  $proc: ${rss} kB"
    done
    echo
  } >>"$MONITOR_LOG"
}

_monitor_resources() {
  : > "$MONITOR_LOG"
  _log_resource_snapshot "monitor start $(date) env=${ENV_NAME}"

  while true; do
    _log_resource_snapshot "$(date)"
    sleep 300
  done
}

# On exit: scan kernel journal for OOM evidence.
# journalctl -k requires read access to kernel logs; falls back to dmesg.
# Always writes something to oom.log so the file is never empty.
_cleanup() {
  _log_resource_snapshot "monitor exit $(date)"

  local found=0
  {
    if journalctl -k --since "7 days ago" \
        | grep -i "oom\|killed process\|out of memory"; then
      found=1
    fi
    if dmesg \
        | grep -i "oom\|killed process\|out of memory"; then
      found=1
    fi
    if [ "$found" -eq 0 ]; then
      echo "No OOM evidence found (checked journalctl and dmesg)"
    fi
  } >>"$OOM_LOG"
}
trap '_cleanup' EXIT

_monitor_resources
