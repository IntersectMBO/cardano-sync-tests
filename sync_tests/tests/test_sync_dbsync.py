"""Tests for cardano-db-sync sync."""

import json
import logging

import pytest

from sync_tests.tests.conftest import DbSyncResult
from sync_tests.tests.conftest import SyncContext

LOGGER = logging.getLogger(__name__)

pytestmark = pytest.mark.db_sync


class TestDbSync:
    """Validate that db-sync synced successfully and produced artifacts."""

    def test_dbsync_reached_tip(
        self,
        db_sync_synced: DbSyncResult,
    ):
        """Check that db-sync tip has valid epoch, block, and slot data."""
        tip = db_sync_synced.db_tip
        LOGGER.info(
            "DB-sync tip: %s",
            json.dumps(tip, indent=2, default=str),
        )
        LOGGER.info(
            "DB-sync sync window: %s -> %s",
            db_sync_synced.db_start_time,
            db_sync_synced.db_end_time,
        )
        LOGGER.info("DB-sync marker: %s", db_sync_synced.db_marker)

        assert tip.get("epoch_no", 0) > 0, "db-sync epoch_no should be > 0 after full sync"
        assert tip.get("block_no", 0) > 0, "db-sync block_no should be > 0 after full sync"
        assert tip.get("slot_no", 0) > 0, "db-sync slot_no should be > 0 after full sync"

    def test_perf_stats_written(
        self,
        db_sync_synced: DbSyncResult,
    ):
        """Check that the performance stats JSON was created and non-empty."""
        perf_path = db_sync_synced.perf_stats_path
        assert perf_path.exists(), f"Performance stats file not found: {perf_path}"
        with open(perf_path) as fh:
            perf_data = json.load(fh)
        assert isinstance(perf_data, list), "perf stats should be a JSON list"
        assert len(perf_data) > 0, "perf stats list should not be empty"
        LOGGER.info(
            "Performance stats file has %d samples: %s",
            len(perf_data),
            perf_path,
        )

    def test_dbsync_marker_written(
        self,
        sync_context: SyncContext,
    ):
        """Check that the db-sync completion marker was written."""
        marker_file = sync_context.marker_file_path
        assert marker_file.exists(), f"Marker file not found: {marker_file}"
        with open(marker_file) as fh:
            data = json.load(fh)
        assert "db_sync_synced" in data, "db_sync_synced entry missing from marker status"
        assert data["db_sync_synced"]["marker"] == ("SYNC_MARKER_DBSYNC_DONE")
        LOGGER.info("DB-sync marker verified in %s", marker_file)
