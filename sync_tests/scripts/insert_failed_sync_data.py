import argparse
import json
import logging
import os

from sync_tests.utils import sync_results_db

LOGGER = logging.getLogger(__name__)


def process_failed_sync_data(backup_filename: str) -> None:
    """Read the JSON backup file and insert the stored data into the database."""
    if not os.path.exists(backup_filename):
        LOGGER.error(f"Backup file {backup_filename} not found. Nothing to process.")
        return

    try:
        with open(backup_filename, encoding="utf-8") as backup_file:
            backup_data = json.load(backup_file)

        LOGGER.info(f"Loaded backup data from {backup_filename}, attempting database insertion.")

        sync_results_db.store_sync_results(sync_data=backup_data)
    except Exception:
        LOGGER.exception(f"Error while processing backup file {backup_filename}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process a failed sync JSON backup file and insert it into the database."
    )
    parser.add_argument(
        "--file",
        type=str,
        required=True,
        help="Path to the backup JSON file (e.g., sync_data_backup.json)",
    )

    args = parser.parse_args()
    process_failed_sync_data(args.file)
