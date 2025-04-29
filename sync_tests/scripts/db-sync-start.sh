#! /usr/bin/env -S nix develop --accept-flake-config .#postgres -i -k LOG_FILEPATH -k ENVIRONMENT -k DB_SYNC_START_ARGS -k POSTGRES_DIR -k PGUSER -k PGPORT -k FIRST_START -c bash
# shellcheck shell=bash

cd cardano-db-sync
export PGPASSFILE="config/pgpass-$ENVIRONMENT"

if [[ $FIRST_START == "True" ]]; then
    cd config
    wget -O "$ENVIRONMENT-db-config.json" "https://book.play.dev.cardano.org/environments/$ENVIRONMENT/db-sync-config.json"
    sed -i "s|NodeConfigFile.*|NodeConfigFile\": \"../../config.json\",|g" "$ENVIRONMENT-db-config.json"
    cd ..
fi

# clear log file
cat /dev/null > "$LOG_FILEPATH"

# wait for node.socket
SOCKET_PATH="../db/node.socket"
MAX_WAIT_SECS=120
WAIT_INTERVAL=5
WAITED=0

echo "Waiting for node socket at $SOCKET_PATH..."

while [ ! -S "$SOCKET_PATH" ]; do
    if (( WAITED >= MAX_WAIT_SECS )); then
        echo "ERROR: Timed out waiting for node socket at $SOCKET_PATH" >&2
        exit 1
    fi
    sleep $WAIT_INTERVAL
    WAITED=$((WAITED + WAIT_INTERVAL))
    echo "Still waiting... ($WAITED/$MAX_WAIT_SECS)"
done

echo "Socket found. Starting db-sync..."

# set abort on first error flag and start db-sync
export DbSyncAbortOnPanic=1
# shellcheck disable=SC2086
PGPASSFILE="$PGPASSFILE" db-sync-node/bin/cardano-db-sync --config "config/$ENVIRONMENT-db-config.json" --socket-path ../db/node.socket --schema-dir schema/ --state-dir "ledger-state/$ENVIRONMENT" ${DB_SYNC_START_ARGS} >> "$LOG_FILEPATH"
