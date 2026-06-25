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
from sync_tests.tests.test_snapshot_creation import snapshot_created  # noqa: F401
from sync_tests.utils import db_sync
from sync_tests.utils import helpers
from sync_tests.utils import node
from sync_tests.utils.logs import log_analyzer

LOGGER = logging.getLogger(__name__)

pytestmark = pytest.mark.local_snapshot

POST_SYNC_WAIT_MINUTES = 20


@pytest.fixture(scope="session")
def local_restoration_result(
    request: FixtureRequest,
    sync_context: SyncContext,
    snapshot_created: dict[str, tp.Any],  # noqa: ARG001,F811
) -> tp.Generator[dict[str, tp.Any], None, None]:
    """Restore db-sync from a local snapshot, sync, and yield result data.

    Args:
        request: Pytest fixture request for CLI options.
        sync_context: Shared session context.
        snapshot_created: Fixture dependency that creates snapshot metadata first.

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
    with open(snapshot_state_file, encoding="utf-8") as state_fh:
        snapshot_data = json.load(state_fh)
    snapshot_file = snapshot_data["snapshot_file"]
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

    # start node
    LOGGER.info("Starting node for post-restoration sync")
    conf_dir = pl.Path.cwd()
    base_dir = pl.Path.cwd()
    bin_dir = pl.Path("bin")
    bin_dir.mkdir(exist_ok=True)
    node.add_to_path(path=bin_dir)

    node.set_node_socket_path_env_var(base_dir=base_dir)
    node.get_node_files(node_rev=node_revision, base_dir=base_dir)
    _cli_version, _cli_git_rev = node.get_node_version()
    node.rm_node_config_files(conf_dir=conf_dir)
    node.get_node_config_files(
        env=env,
        node_topology_type="",
        conf_dir=conf_dir,
        disable_genesis_mode_flag=False,
    )
    node.configure_node(config_file=conf_dir / "config.json")
    node.start_node(
        base_dir=base_dir,
        node_start_arguments=(),
        logfile_path=config.node_log_file,
    )
    node.wait_node_start(
        env=env,
        base_dir=base_dir,
        timeout_minutes=10,
        logfile_path=config.node_log_file,
    )
    helpers.print_last_n_lines(config.node_log_file, 80)
    node.wait_for_node_to_sync(env=env, base_dir=base_dir)

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
    helpers.manage_process(
        proc_name="cardano-db-sync",
        action="terminate",
    )
    helpers.manage_process(
        proc_name="cardano-node",
        action="terminate",
    )
    node.rm_node_db_dir(base_dir=base_dir)
    db_sync.stop_postgres(config)
    db_sync.finalize_session_disk_cleanup(config)


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
