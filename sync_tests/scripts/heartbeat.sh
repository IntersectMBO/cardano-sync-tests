#! /usr/bin/env bash
# shellcheck shell=bash disable=SC2317
# heartbeat.sh [combined|node-only]
#
# Periodic GitHub Actions UI heartbeat (foldable ::group:: blocks).
#
# Usage:
#   bash heartbeat.sh combined

set -uo pipefail

trap - ERR EXIT SIGINT 2>/dev/null || true

MODE="${1:-combined}"
WORKDIR="${SYNC_TESTS_HEARTBEAT_WORKDIR:-test_workdir}"
INTERVAL="${SYNC_TESTS_HEARTBEAT_INTERVAL_SEC:-600}"
TAIL_LINES="${SYNC_TESTS_HEARTBEAT_TAIL_LINES:-30}"
HALF_TAIL=$(( TAIL_LINES / 2 ))

_tail_group() {
  local label="$1"
  local file="$2"
  local n="$3"
  if [ -f "$file" ]; then
    echo "::group::${label} (last ${n})"
    tail -n "$n" "$file"
    echo "::endgroup::"
  else
    echo "[${label} not present yet]"
  fi
}

_tail_ci_step_group() {
  local file="$1"
  local n="$2"
  local label="ci_step.log (startup)"

  if [ ! -f "$file" ]; then
    echo "[ci_step.log not present yet: waiting for CI step output]"
    return
  fi
  if [ ! -s "$file" ] || [ -z "$(tr -d '[:space:]' < "$file")" ]; then
    echo "::group::${label} (last ${n})"
    echo "[ci_step.log empty: startup likely finished before this heartbeat; no issue if sync logs are present]"
    echo "::endgroup::"
    return
  fi
  _tail_group "$label" "$file" "$n"
}

_tail_build_logs() {
  local ci_file="$1"
  local nix_file="$2"
  local n="$3"
  _tail_ci_step_group "$ci_file" "$n"
  if [ -f "$nix_file" ] && [ -s "$nix_file" ]; then
    _tail_group "nix_build.log" "$nix_file" "$n"
  fi
}

_heartbeat_tick() {
  local node_log="$WORKDIR/node_sync.log"
  local db_log="$WORKDIR/db_sync.log"
  local ci_step_log="$WORKDIR/ci_step.log"
  local nix_build_log="$WORKDIR/nix_build.log"
  local node_done=0 db_done=0
  local phase="" status=""

  shopt -s nullglob
  local markers_files=( "$WORKDIR"/sync_markers_*.json )
  shopt -u nullglob
  if [ "${#markers_files[@]}" -gt 0 ]; then
    grep -q SYNC_MARKER_NODE_DONE   "${markers_files[@]}" 2>/dev/null && node_done=1
    grep -q SYNC_MARKER_DBSYNC_DONE "${markers_files[@]}" 2>/dev/null && db_done=1
  fi

  if [ "$MODE" = "node-only" ]; then
    if [ "$node_done" = "1" ]; then
      phase="node done"
      status="node=done"
    elif [ -f "$node_log" ]; then
      phase="node syncing"
      status="node=syncing"
    else
      phase="build - waiting for node log"
      status="node=not-started"
    fi
  elif [ "$node_done" = "1" ] && [ "$db_done" = "1" ]; then
    phase="post-sync - node done - db done"
    status="node=done db=done"
  elif [ "$node_done" = "1" ]; then
    if [ -f "$db_log" ]; then
      phase="db-sync running - node done"
      status="node=done db=syncing"
    else
      phase="db-sync starting - node done"
      status="node=done db=not-started"
    fi
  elif [ -f "$node_log" ] && [ -f "$db_log" ]; then
    phase="node+db syncing"
    status="node=syncing db=syncing"
  elif [ -f "$node_log" ]; then
    phase="node syncing - db waiting"
    status="node=syncing db=waiting-for-node"
  elif [ -f "$db_log" ]; then
    phase="db syncing - node unknown"
    status="node=unknown db=syncing"
  else
    phase="build - waiting for logs"
    status="node=not-started db=not-started"
  fi

  echo "::group::Heartbeat [${phase}] $(date -u +%FT%TZ)"
  echo "status: ${status}"
  df -h "${GITHUB_WORKSPACE:-.}" 2>/dev/null || df -h . 2>/dev/null || echo "[df unavailable]"

  if [ "$MODE" = "node-only" ]; then
    if [ -f "$node_log" ]; then
      _tail_group "node_sync.log" "$node_log" "$TAIL_LINES"
    else
      _tail_build_logs "$ci_step_log" "$nix_build_log" "$TAIL_LINES"
    fi
  elif [ "$node_done" = "1" ] && [ "$db_done" = "1" ]; then
    _tail_group "db_sync.log" "$db_log" "$TAIL_LINES"
  elif [ "$node_done" = "1" ]; then
    local tip_line=""
    if [ -f "$node_log" ]; then
      tip_line="$(grep -E 'Chain extended|new tip' "$node_log" 2>/dev/null | tail -n 1)"
    fi
    echo "[node caught up] ${tip_line:-<no tip line found>}"
    _tail_group "db_sync.log" "$db_log" "$TAIL_LINES"
  elif [ -f "$node_log" ] || [ -f "$db_log" ]; then
    _tail_group "node_sync.log" "$node_log" "$HALF_TAIL"
    _tail_group "db_sync.log" "$db_log" "$HALF_TAIL"
  else
    _tail_build_logs "$ci_step_log" "$nix_build_log" "$TAIL_LINES"
  fi
  echo "::endgroup::"
}

_heartbeat_tick
while sleep "$INTERVAL"; do
  _heartbeat_tick
done
