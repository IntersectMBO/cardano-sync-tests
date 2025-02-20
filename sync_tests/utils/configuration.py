import pathlib as pl
import os
import subprocess

ROOT_TEST_PATH = pl.Path.cwd()
ENVIRONMENT = os.getenv("environment")

# System Information
POSTGRES_DIR = ROOT_TEST_PATH.parents[0]
POSTGRES_USER = (
    subprocess.run(["whoami"], stdout=subprocess.PIPE, check=False).stdout.decode("utf-8").strip()
)

# Log and Stats Paths
db_sync_perf_stats: list[dict] = []
DB_SYNC_PERF_STATS_FILE = (
    ROOT_TEST_PATH / f"cardano-db-sync/db_sync_{ENVIRONMENT}_performance_stats.json"
)
DB_SYNC_LOG_FILE = ROOT_TEST_PATH / f"cardano-db-sync/db_sync_{ENVIRONMENT}_logfile.log"
EPOCH_SYNC_TIMES_FILE = ROOT_TEST_PATH / f"cardano-db-sync/epoch_sync_times_{ENVIRONMENT}_dump.json"
NODE_LOG_FILE = ROOT_TEST_PATH / f"cardano-node/node_{ENVIRONMENT}_logfile.log"

# Archive Names
NODE_ARCHIVE_NAME = f"cardano_node_{ENVIRONMENT}_logs.zip"
DB_SYNC_ARCHIVE_NAME = f"cardano_db_sync_{ENVIRONMENT}_logs.zip"
SYNC_DATA_ARCHIVE_NAME = f"epoch_sync_times_{ENVIRONMENT}_dump.zip"
PERF_STATS_ARCHIVE_NAME = f"db_sync_{ENVIRONMENT}_perf_stats.zip"
