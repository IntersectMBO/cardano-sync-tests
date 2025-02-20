#! /usr/bin/env -S nix develop --accept-flake-config .#python -c bash
# shellcheck shell=bash

set -xeo pipefail

python3 -c "import requests, pandas, psutil, pymysql;"

env="$1"
shift

echo " ==== Start sync test"
python ./sync_tests/tests/node_sync_test.py "$@"

echo "--- Prepare for adding sync test results to the AWS Database"
python ./sync_tests/tests/node_write_sync_values_to_db.py -e "$env"
