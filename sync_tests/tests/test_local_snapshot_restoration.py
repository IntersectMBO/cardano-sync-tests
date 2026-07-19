"""Tests for db-sync restoration from a locally created snapshot."""

from __future__ import annotations

import datetime
import json
import logging
import os
import pathlib as pl
import time
import typing as tp
from collections import OrderedDict

import pytest
from _pytest.fixtures import FixtureRequest

from sync_tests.tests.conftest import SyncContext
from sync_tests.utils import db_sync
from sync_tests.utils import helpers
from sync_tests.utils import sync_entries
from sync_tests.utils.logs import log_analyzer

LOGGER = logging.getLogger(__name__)

pytestmark = pytest.mark.local_snapshot

POST_SYNC_WAIT_MINUTES = 20


@pytest.fixture(scope="session")
def local_restoration_result(
    request: FixtureRequest,
    sync_context: SyncContext,
) -> tp.Generator[dict[str, tp.Any], None, None]:
    """Restore db-sync from a local snapshot, sync, and yield result data.

    Reads snapshot metadata written by a prior snapshot creation run. Run the
    snapshot_creation marked tests first to produce that metadata.

    Args:
        request: Pytest fixture request for CLI options.
        sync_context: Shared session context.

    Yields:
        Dict with restoration timing, tip data, and sync results.
    """
    if request.config.getoption("--run-only-sync-test"):
        pytest.skip("--run-only-sync-test: skipping snapshot restoration")

    env = sync_context.env
    node_revision: str | None = request.config.getoption("--node-revision")
    db_sync_revision: str | None = request.config.getoption(
        "--db-sync-revision",
    )
    pg_port: str = request.config.getoption("--pg-port") or "5433"

    if not node_revision:
        pytest.skip("--node-revision required for local snapshot restoration")
    if not db_sync_revision:
        pytest.skip(
            "--db-sync-revision required for local snapshot restoration",
        )

    config = db_sync.create_db_sync_config(
        env=env,
        workdir=sync_context.workdir,
        pg_port=pg_port,
    )

    platform_system, platform_release, platform_version = helpers.get_os_type()
    start_test_time = datetime.datetime.now(
        tz=datetime.timezone.utc,
    ).strftime("%d/%m/%Y %H:%M:%S")

    # database setup
    LOGGER.info("Local snapshot restoration: postgres and database setup")
    db_sync.setup_postgres(config, pg_port=pg_port)
    db_sync.create_pgpass_file(config)
    db_sync.create_database()

    # restore snapshot
    snapshot_state_file = sync_context.workdir / "sync_session_state.json"
    if not snapshot_state_file.exists():
        msg = f"Snapshot metadata file not found: {snapshot_state_file}"
        raise FileNotFoundError(msg)
    try:
        with open(snapshot_state_file, encoding="utf-8") as state_fh:
            snapshot_data = json.load(state_fh)
        snapshot_file = snapshot_data["snapshot_file"]
    except (json.JSONDecodeError, KeyError) as exc:
        msg = f"Invalid snapshot metadata in {snapshot_state_file}: {exc}"
        raise ValueError(msg) from exc
    LOGGER.info("Restoring from snapshot: %s", snapshot_file)
    restoration_time = db_sync.restore_db_sync_from_snapshot(
        config,
        snapshot_file,
    )
    LOGGER.info("Restoration time: %d seconds", restoration_time)

    db_sync_tip = db_sync.get_db_sync_tip(config)
    if db_sync_tip is None:
        msg = "db-sync tip unavailable after snapshot restoration"
        raise RuntimeError(msg)
    snapshot_epoch_no = db_sync_tip.epoch_no
    snapshot_block_no = db_sync_tip.block_no
    snapshot_slot_no = db_sync_tip.slot_no
    LOGGER.info(
        "Tip after restoration: epoch=%s block=%s slot=%s",
        snapshot_epoch_no,
        snapshot_block_no,
        snapshot_slot_no,
    )

    # start node (reuses conftest's run_node_sync instead of hand-rolling the same
    # build+start+wait-for-sync sequence a third time; clean_start=False preserves
    # this test's original behavior of reusing an already-synced node db if one is
    # already present from an earlier step in the same session)
    LOGGER.info("Starting node for post-restoration sync")
    base_dir = pl.Path.cwd()
    sync_entries.run_node_sync(
        env=env,
        node_revision=node_revision,
        node_logfile_path=config.node_log_file,
        base_dir=base_dir,
        conf_dir=base_dir,
        start_era="shelley",
        clean_start=False,
        full_sync=True,
    )

    # start db-sync
    LOGGER.info("Starting db-sync after snapshot restoration")
    helpers.export_env_var("PGPORT", pg_port)
    db_sync.start_db_sync(config, start_args="", first_start="False")
    helpers.print_last_n_lines(config.db_sync_log_file, 20)
    time.sleep(60)
    db_sync_version, db_sync_git_rev = db_sync.get_db_sync_version(config)
    db_full_sync_time_in_secs, _perf_stats = db_sync.wait_for_db_to_sync(
        config,
    )

    end_test_time = datetime.datetime.now(
        tz=datetime.timezone.utc,
    ).strftime("%d/%m/%Y %H:%M:%S")

    LOGGER.info(
        "Waiting %d minutes for additional syncing",
        POST_SYNC_WAIT_MINUTES,
    )
    time.sleep(POST_SYNC_WAIT_MINUTES * 60)

    db_sync_tip = db_sync.get_db_sync_tip(config)
    if db_sync_tip is None:
        msg = "db-sync tip unavailable after post-restore sync wait"
        raise RuntimeError(msg)

    result: dict[str, tp.Any] = {
        "platform_system": platform_system,
        "platform_release": platform_release,
        "platform_version": platform_version,
        "env": env,
        "db_sync_version": db_sync_version,
        "db_sync_git_rev": db_sync_git_rev,
        "start_test_time": start_test_time,
        "end_test_time": end_test_time,
        "db_full_sync_time_in_secs": db_full_sync_time_in_secs,
        "snapshot_file": snapshot_file,
        "restoration_time": restoration_time,
        "snapshot_epoch_no": snapshot_epoch_no,
        "snapshot_block_no": snapshot_block_no,
        "snapshot_slot_no": snapshot_slot_no,
        "last_synced_epoch_no": db_sync_tip.epoch_no,
        "last_synced_block_no": db_sync_tip.block_no,
        "last_synced_slot_no": db_sync_tip.slot_no,
        "config": config,
    }

    yield result

    LOGGER.info("Teardown: terminating cardano services")
    sync_entries.teardown_node_and_db_sync(base_dir=base_dir, config=config)


class TestLocalSnapshotRestoration:
    """Validate db-sync restoration from a locally created snapshot."""

    def test_restoration_completed(
        self,
        local_restoration_result: dict[str, tp.Any],
    ):
        """Check that the snapshot was restored and db-sync re-synced."""
        assert local_restoration_result["restoration_time"] > 0
        assert local_restoration_result["db_full_sync_time_in_secs"] >= 0
        assert (
            local_restoration_result["last_synced_epoch_no"]
            >= (local_restoration_result["snapshot_epoch_no"])
        )

    def test_restoration_results_json(
        self,
        sync_context: SyncContext,
        local_restoration_result: dict[str, tp.Any],
    ):
        """Generate and upload restoration test results JSON."""
        config = local_restoration_result["config"]
        test_results_file = (
            config.workdir / f"db_sync_{sync_context.env}"
            f"_local_snapshot_restoration_test_results.json"
        )

        test_data: OrderedDict[str, tp.Any] = OrderedDict()
        test_data["platform_system"] = local_restoration_result["platform_system"]
        test_data["platform_release"] = local_restoration_result["platform_release"]
        test_data["platform_version"] = local_restoration_result["platform_version"]
        test_data["no_of_cpu_cores"] = os.cpu_count()
        test_data["total_ram_in_GB"] = helpers.get_total_ram_in_gb()
        test_data["env"] = local_restoration_result["env"]
        test_data["db_sync_version"] = local_restoration_result["db_sync_version"]
        test_data["db_sync_git_rev"] = local_restoration_result["db_sync_git_rev"]
        test_data["start_test_time"] = local_restoration_result["start_test_time"]
        test_data["end_test_time"] = local_restoration_result["end_test_time"]
        test_data["db_total_sync_time_in_sec"] = local_restoration_result[
            "db_full_sync_time_in_secs"
        ]
        test_data["db_total_sync_time_in_h_m_s"] = str(
            datetime.timedelta(
                seconds=int(local_restoration_result["db_full_sync_time_in_secs"]),
            )
        )
        test_data["snapshot_name"] = local_restoration_result["snapshot_file"]
        test_data["snapshot_size_in_mb"] = db_sync.get_file_size(
            local_restoration_result["snapshot_file"],
        )
        test_data["restoration_time"] = local_restoration_result["restoration_time"]
        test_data["snapshot_epoch_no"] = local_restoration_result["snapshot_epoch_no"]
        test_data["snapshot_block_no"] = local_restoration_result["snapshot_block_no"]
        test_data["snapshot_slot_no"] = local_restoration_result["snapshot_slot_no"]
        test_data["last_synced_epoch_no"] = local_restoration_result["last_synced_epoch_no"]
        test_data["last_synced_block_no"] = local_restoration_result["last_synced_block_no"]
        test_data["last_synced_slot_no"] = local_restoration_result["last_synced_slot_no"]
        test_data["total_database_size"] = db_sync.get_total_db_size(config)
        test_data["rollbacks"] = log_analyzer.is_string_present_in_file(
            file_to_check=config.db_sync_log_file,
            search_string="rolling back to",
        )
        test_data["errors"] = log_analyzer.is_string_present_in_file(
            file_to_check=config.db_sync_log_file,
            search_string="db-sync-node:Error",
        )

        helpers.write_json_to_file(test_results_file, test_data)

        archive_name = f"cardano_db_sync_{sync_context.env}_restoration.zip"
        helpers.zip_file(archive_name, config.db_sync_log_file)
        db_sync.upload_artifact(archive_name)
        db_sync.upload_artifact(str(test_results_file))

        assert test_results_file.exists()

    def test_restoration_log_analysis(
        self,
        local_restoration_result: dict[str, tp.Any],
    ):
        """Check db-sync logs for errors after snapshot restoration."""
        config = local_restoration_result["config"]
        log_analyzer.check_db_sync_logs(log_file=config.db_sync_log_file)
