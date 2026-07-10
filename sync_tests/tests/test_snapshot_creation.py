"""Tests for db-sync snapshot creation."""

from __future__ import annotations

import datetime
import logging
import os
import typing as tp
from collections import OrderedDict

import pytest
from _pytest.fixtures import FixtureRequest

from sync_tests.tests.conftest import DbSyncResult
from sync_tests.tests.conftest import SyncContext
from sync_tests.utils import db_sync
from sync_tests.utils import helpers

LOGGER = logging.getLogger(__name__)

pytestmark = pytest.mark.snapshot_creation


@pytest.fixture(scope="session")
def snapshot_created(
    request: FixtureRequest,
    sync_context: SyncContext,
    db_sync_synced: DbSyncResult,  # noqa: ARG001
) -> dict[str, tp.Any]:
    """Create a db-sync snapshot and return creation metadata."""
    if request.config.getoption("--run-only-sync-test"):
        pytest.skip("--run-only-sync-test: skipping snapshot creation")

    env = sync_context.env
    config = db_sync.create_db_sync_config(
        env=env,
        workdir=sync_context.workdir,
    )

    LOGGER.info("Starting snapshot creation")
    start_time = datetime.datetime.now(tz=datetime.timezone.utc)
    stage_2_cmd = db_sync.create_db_sync_snapshot_stage_1(config)
    LOGGER.info("Stage 2 command: %s", stage_2_cmd)
    stage_2_result = db_sync.create_db_sync_snapshot_stage_2(
        config,
        stage_2_cmd,
    )
    LOGGER.info("Stage 2 result: %s", stage_2_result)
    end_time = datetime.datetime.now(tz=datetime.timezone.utc)

    snapshot_file = stage_2_result

    creation_secs = int((end_time - start_time).total_seconds())
    LOGGER.info("Snapshot creation time: %d seconds", creation_secs)

    snapshot_data = {
        "snapshot_file": snapshot_file,
        "stage_2_cmd": stage_2_cmd,
        "stage_2_result": stage_2_result,
        "creation_time_secs": creation_secs,
        "start_time": start_time.strftime("%d/%m/%Y %H:%M:%S"),
        "end_time": end_time.strftime("%d/%m/%Y %H:%M:%S"),
    }
    helpers.write_json_to_file(sync_context.workdir / "sync_session_state.json", snapshot_data)
    return snapshot_data


class TestSnapshotCreation:
    """Validate db-sync snapshot creation."""

    def test_snapshot_created(
        self,
        snapshot_created: dict[str, tp.Any],
    ):
        """Check that the snapshot file was created successfully."""
        assert snapshot_created["snapshot_file"], "Snapshot file name is empty"
        assert snapshot_created["creation_time_secs"] > 0

    def test_snapshot_results_json(
        self,
        sync_context: SyncContext,
        db_sync_synced: DbSyncResult,
        snapshot_created: dict[str, tp.Any],
    ):
        """Generate and upload snapshot creation test results."""
        env = sync_context.env
        config = db_sync.create_db_sync_config(
            env=env,
            workdir=sync_context.workdir,
        )
        test_results_file = config.workdir / f"snapshot_creation_{env}_test_results.json"

        platform_system, platform_release, platform_version = helpers.get_os_type()

        test_data: OrderedDict[str, tp.Any] = OrderedDict()
        test_data["platform_system"] = platform_system
        test_data["platform_release"] = platform_release
        test_data["platform_version"] = platform_version
        test_data["no_of_cpu_cores"] = os.cpu_count()
        test_data["total_ram_in_GB"] = helpers.get_total_ram_in_gb()
        test_data["env"] = env
        test_data["db_sync_version"] = db_sync_synced.db_sync_version
        test_data["db_sync_git_rev"] = db_sync_synced.db_sync_git_rev
        test_data["start_test_time"] = snapshot_created["start_time"]
        test_data["end_test_time"] = snapshot_created["end_time"]
        test_data["snapshot_creation_time_in_sec"] = snapshot_created["creation_time_secs"]
        test_data["snapshot_creation_time_in_h_m_s"] = str(
            datetime.timedelta(
                seconds=snapshot_created["creation_time_secs"],
            )
        )
        test_data["snapshot_size_in_mb"] = db_sync.get_file_size(
            snapshot_created["snapshot_file"],
        )
        test_data["stage_2_cmd"] = snapshot_created["stage_2_cmd"]
        test_data["stage_2_result"] = snapshot_created["stage_2_result"]

        helpers.write_json_to_file(test_results_file, test_data)
        db_sync.upload_artifact(str(test_results_file))

        if env != "mainnet":
            db_sync.upload_artifact(snapshot_created["snapshot_file"])

        assert test_results_file.exists()
