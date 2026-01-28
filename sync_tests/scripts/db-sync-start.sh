#! /usr/bin/env bash
# shellcheck shell=bash
# Note: Removed nix develop wrapper - db-sync binary is already built and available
# The script now runs directly without nix develop, which was failing because .#postgres devShell doesn't exist

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT/cardano-db-sync" || exit 1
export PGPASSFILE="config/pgpass-$ENVIRONMENT"
CONFIG_DIR="$REPO_ROOT/cardano-db-sync/config"

if [[ $FIRST_START == "True" ]]; then
    (
        cd "$CONFIG_DIR" || exit 1
        wget -O "$ENVIRONMENT-db-config.json" "https://book.world.dev.cardano.org/environments/$ENVIRONMENT/db-sync-config.json"
        # The sync-tests harness starts the node in the workdir and uses `config.json` at repo root.
        # Prefer that layout, but keep fallback for the older `cardano-node/<env>-config.json` layout.
        if [[ -f "$REPO_ROOT/config.json" ]]; then
            # Use absolute path to avoid any relative path resolution issues
            # Try readlink -f first, fallback to realpath or manual resolution
            if command -v readlink >/dev/null 2>&1; then
                ABS_CONFIG_PATH=$(
                    readlink -f "$REPO_ROOT/config.json" 2>/dev/null || realpath "$REPO_ROOT/config.json" 2>/dev/null || echo "$REPO_ROOT/config.json"
                )
            else
                ABS_CONFIG_PATH="$REPO_ROOT/config.json"
            fi
            # Match the exact pattern in the JSON file and replace it
            sed -i "s|\"NodeConfigFile\": \".*\"|\"NodeConfigFile\": \"$ABS_CONFIG_PATH\"|g" "$ENVIRONMENT-db-config.json"
        else
            # Fallback to older layout
            sed -i "s|\"NodeConfigFile\": \".*\"|\"NodeConfigFile\": \"..\\/..\\/cardano-node\\/$ENVIRONMENT-config.json\"|g" "$ENVIRONMENT-db-config.json"
        fi
    )
fi

# clear log file
cat /dev/null > "$LOG_FILEPATH"

# set abort on first error flag and start db-sync
export DbSyncAbortOnPanic=1
# Resolve socket path to absolute path to avoid relative path resolution issues
# First, check if CARDANO_NODE_SOCKET_PATH is set (preferred method - always use if set)
if [[ -n "${CARDANO_NODE_SOCKET_PATH:-}" ]]; then
    # Use the environment variable if set (even if socket doesn't exist yet - node will create it)
    if command -v readlink >/dev/null 2>&1; then
        NODE_SOCKET_PATH=$(readlink -f "$CARDANO_NODE_SOCKET_PATH" 2>/dev/null || realpath "$CARDANO_NODE_SOCKET_PATH" 2>/dev/null || echo "$CARDANO_NODE_SOCKET_PATH")
    else
        NODE_SOCKET_PATH="$CARDANO_NODE_SOCKET_PATH"
    fi
# Otherwise, search for the socket in common locations
elif [[ -f "../test_workdir/db/node.socket" ]] || [[ -S "../test_workdir/db/node.socket" ]]; then
    # Node is in test_workdir (from cardano-db-sync, go up one level, then test_workdir/db/node.socket)
    if command -v readlink >/dev/null 2>&1; then
        NODE_SOCKET_PATH=$(readlink -f "../test_workdir/db/node.socket" 2>/dev/null || realpath "../test_workdir/db/node.socket" 2>/dev/null || echo "$(cd .. && pwd)/test_workdir/db/node.socket")
    else
        NODE_SOCKET_PATH="$(cd .. && pwd)/test_workdir/db/node.socket"
    fi
elif [[ -f "../db/node.socket" ]] || [[ -S "../db/node.socket" ]]; then
    # Node is in workdir (from cardano-db-sync, go up one level to workdir, then db/node.socket)
    if command -v readlink >/dev/null 2>&1; then
        NODE_SOCKET_PATH=$(readlink -f "../db/node.socket" 2>/dev/null || realpath "../db/node.socket" 2>/dev/null || echo "$(cd .. && pwd)/db/node.socket")
    else
        NODE_SOCKET_PATH="$(cd .. && pwd)/db/node.socket"
    fi
elif [[ -f "../cardano-node/db/node.socket" ]] || [[ -S "../cardano-node/db/node.socket" ]]; then
    # Fallback to older layout
    if command -v readlink >/dev/null 2>&1; then
        NODE_SOCKET_PATH=$(readlink -f "../cardano-node/db/node.socket" 2>/dev/null || realpath "../cardano-node/db/node.socket" 2>/dev/null || echo "$(cd ../cardano-node && pwd)/db/node.socket")
    else
        NODE_SOCKET_PATH="$(cd ../cardano-node && pwd)/db/node.socket"
    fi
else
    # Default to workdir/db/node.socket (absolute)
    NODE_SOCKET_PATH="$(cd .. && pwd)/db/node.socket"
fi
db_sync_args=()
if [[ -n "${DB_SYNC_START_ARGS:-}" ]]; then
    read -r -a db_sync_args <<< "$DB_SYNC_START_ARGS"
fi
PGPASSFILE="$PGPASSFILE" db-sync-node/bin/cardano-db-sync --config "config/$ENVIRONMENT-db-config.json" --socket-path "$NODE_SOCKET_PATH" --schema-dir schema/ --state-dir "ledger-state/$ENVIRONMENT" "${db_sync_args[@]}" >> "$LOG_FILEPATH" 2>&1 &
