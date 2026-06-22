import argparse
import json
import logging
import os

LOGGER = logging.getLogger(__name__)


def process_failed_sync_data(backup_filename: str) -> None:
    """Read the JSON backup file and insert the stored data into the database."""
    if not os.path.exists(backup_filename):
        LOGGER.error("Backup file %s not found. Nothing to process.", backup_filename)
        return

    try:
        with open(backup_filename, encoding="utf-8") as backup_file:
            backup_data = json.load(backup_file)

        LOGGER.info(
            "Loaded backup data from %s. AWS DB uploads removed; saving locally instead.",
            backup_filename,
        )
        output_file = "sync_data_reprocessed.json"
        with open(output_file, "w", encoding="utf-8") as out_file:
            json.dump(backup_data, out_file, indent=2)
        LOGGER.info("Wrote backup data to %s", output_file)
    except Exception:
        LOGGER.exception("Error while processing backup file %s", backup_filename)


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
