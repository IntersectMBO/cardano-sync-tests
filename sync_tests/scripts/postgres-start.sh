#! /usr/bin/env -S nix develop --accept-flake-config .#postgres -i -k PGHOST -k PGPORT -k PGUSER -c bash
# shellcheck shell=bash

set -euo pipefail

POSTGRES_DIR="${1:?"Need path to postgres dir"}"
POSTGRES_DIR="$(readlink -m "$POSTGRES_DIR")"

# Postgres refuses to start if the full Unix socket path exceeds ~107 bytes
# (directory + "/.s.PGSQL.<port>"). Long CI workdirs need a
# short -k directory; PGDATA stays under POSTGRES_DIR/data.
_MAX_SOCK_PARENT_LEN=$((107 - 22))
SOCKET_DIR="$POSTGRES_DIR"
if [ "${#POSTGRES_DIR}" -gt "$_MAX_SOCK_PARENT_LEN" ]; then
  _sock_hash="$(printf '%s' "$POSTGRES_DIR" | sha256sum | awk '{print $1}' | cut -c1-16)"
  SOCKET_DIR="/tmp/syncpg-$_sock_hash"
fi

# set postgres env variables
export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-postgres}"

# kill running postgres and clear its data
if [ "${2:-""}" = "-k" ]; then
  # try to kill whatever is listening on postgres port
  listening_pid="$(lsof -i:"$PGPORT" -t || echo "")"
  if [ -n "$listening_pid" ]; then
    if ! kill "$listening_pid"; then
      echo "Warning: failed to kill PID $listening_pid on port $PGPORT." >&2
      echo "Another postgres may be running. Stop it or set PGPORT to a free port." >&2
    fi
  fi

  if [ "$SOCKET_DIR" != "$POSTGRES_DIR" ]; then
    rm -rf "$SOCKET_DIR"
  fi
  rm -rf "$POSTGRES_DIR/data"
fi

# setup db
if [ ! -e "$POSTGRES_DIR/data" ]; then
  mkdir -p "$POSTGRES_DIR/data"
  initdb -D "$POSTGRES_DIR/data" --encoding=UTF8 --locale=en_US.UTF-8 -A trust -U "$PGUSER"

  # Tune postgres relative to the host's actual RAM instead of assuming a
  # fixed-size CI runner. Defaults (e.g. shared_buffers=128MB) are far too
  # conservative for a ~1 TB mainnet database and cause OOM kills at epoch
  # boundaries; a hardcoded 8GB/2GB in turn can fail to start or get OOM
  # killed on a smaller host. Scale off detected total memory, with the
  # scaling factors chosen so a 64 GB host lands on the previous fixed
  # values (8GB / 2GB) unchanged. Override with PG_SHARED_BUFFERS /
  # PG_MAINTENANCE_WORK_MEM (postgresql.conf memory syntax, e.g. "4GB").
  _total_mem_kb="$(awk '/MemTotal/ {print $2}' /proc/meminfo 2> /dev/null || true)"
  if [ -z "$_total_mem_kb" ]; then
    echo "Warning: could not read /proc/meminfo, assuming 64GB host RAM." >&2
    _total_mem_kb=$((64 * 1024 * 1024))
  fi
  _total_mem_gb=$(( _total_mem_kb / 1024 / 1024 ))
  [ "$_total_mem_gb" -lt 1 ] && _total_mem_gb=1

  _default_shared_buffers_gb=$(( _total_mem_gb / 8 ))
  [ "$_default_shared_buffers_gb" -lt 1 ] && _default_shared_buffers_gb=1
  _default_maintenance_work_mem_gb=$(( _total_mem_gb / 32 ))
  [ "$_default_maintenance_work_mem_gb" -lt 1 ] && _default_maintenance_work_mem_gb=1

  PG_SHARED_BUFFERS="${PG_SHARED_BUFFERS:-${_default_shared_buffers_gb}GB}"
  PG_MAINTENANCE_WORK_MEM="${PG_MAINTENANCE_WORK_MEM:-${_default_maintenance_work_mem_gb}GB}"

  echo "Host RAM: ${_total_mem_gb}GB detected; shared_buffers=${PG_SHARED_BUFFERS} maintenance_work_mem=${PG_MAINTENANCE_WORK_MEM}"

  cat >> "$POSTGRES_DIR/data/postgresql.conf" << EOF

# Sync-test tuning, scaled off detected host RAM (${_total_mem_gb}GB)
shared_buffers = ${PG_SHARED_BUFFERS}
maintenance_work_mem = ${PG_MAINTENANCE_WORK_MEM}
max_parallel_maintenance_workers = 2
max_wal_size = 8GB
wal_compression = on
EOF
fi

# start postgres
mkdir -p "$SOCKET_DIR"
postgres -D "$POSTGRES_DIR/data" -k "$SOCKET_DIR" > "$POSTGRES_DIR/postgres.log" 2>&1 &
PSQL_PID="$!"
sleep 5
cat "$POSTGRES_DIR/postgres.log"
echo
ps -fp "$PSQL_PID"

if grep -E -q "could not bind IPv4 address|could not create any TCP/IP sockets|could not create any Unix-domain sockets|too long \(maximum" "$POSTGRES_DIR/postgres.log"; then
  echo "Postgres failed to start: port $PGPORT is already in use or Unix socket path is invalid." >&2
  echo "Stop the system postgres or export PGPORT=<free_port> before running tests." >&2
  exit 1
fi
