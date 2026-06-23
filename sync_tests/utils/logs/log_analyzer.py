"""Log file analysis utilities for DB Sync and node log files."""

from __future__ import annotations

import logging
import mmap
import pathlib as pl
import re

LOGGER = logging.getLogger(__name__)

ROOT_TEST_PATH = pl.Path.cwd()
DB_SYNC_LOG_FILE = ROOT_TEST_PATH / "test_workdir" / "db_sync.log"


def is_string_present_in_file(file_to_check: str | pl.Path, search_string: str) -> bool:
    """Check if a specific string is present in a given file."""
    pattern = re.escape(search_string)
    with open(file_to_check, encoding="utf-8") as file:
        return any(re.search(pattern, line) for line in file)


def are_rollbacks_present_in_logs(log_file: str | pl.Path) -> bool:
    """Check for rollbacks in the logs."""
    log_file = pl.Path(log_file)
    search_term = b"rolling"

    with (
        open(log_file, "rb", 0) as file,
        mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as mm,
    ):
        initial_rollback_position = mm.find(search_term)

        if initial_rollback_position == -1:
            return False  # No "rolling" found at all

        next_rolling_position = mm.find(search_term, initial_rollback_position + len(search_term))
        return next_rolling_position != -1  # True if found, False otherwise


def check_db_sync_logs(log_file: str | pl.Path = DB_SYNC_LOG_FILE) -> None:
    """Search for error indicators in the database synchronization logs and logs the results."""
    LOGGER.info("Checking DB sync logs for errors, rollbacks, and other potential issues.")
    if is_string_present_in_file(file_to_check=log_file, search_string="db-sync-node:Error"):
        LOGGER.warning("Errors present in %s", log_file)

    if are_rollbacks_present_in_logs(log_file=log_file):
        LOGGER.warning("Rollbacks present in %s", log_file)

    if is_string_present_in_file(file_to_check=log_file, search_string="Rollback failed"):
        LOGGER.warning("Failed rollbacks present in %s", log_file)

    if is_string_present_in_file(
        file_to_check=log_file, search_string="Failed to parse ledger state"
    ):
        LOGGER.warning("Corrupted ledger files present in %s", log_file)
