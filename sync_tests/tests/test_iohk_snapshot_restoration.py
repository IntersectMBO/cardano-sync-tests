"""Tests for db-sync restoration from an IOHK official snapshot."""

from __future__ import annotations

import datetime
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
from sync_tests.utils import node
from sync_tests.utils.external import gitpython
from sync_tests.utils.logs import log_analyzer

LOGGER = logging.getLogger(__name__)

pytestmark = pytest.mark.iohk_snapshot

POST_SYNC_WAIT_MINUTES = 30


@pytest.fixture(scope="session")
def iohk_restoration_result(
    request: FixtureRequest,
    sync_context: SyncContext,
) -> tp.Generator[dict[str, tp.Any], None, None]:
    """Download IOHK snapshot, restore, sync node + db-sync, yield results.

    Args:
        request: Pytest fixture request for CLI options.
        sync_context: Shared session context.

    Yields:
        Dict with restoration timing, tip data, versions, and sync results.
    """
    env = sync_context.env
    node_revision: str | None = request.config.getoption("--node-revision")
    db_sync_revision: str | None = request.config.getoption(
        "--db-sync-revision",
    )
    snapshot_url_opt: str | None = request.config.getoption("--snapshot-url")

    if not node_revision:
        pytest.skip(
            "--node-revision required for IOHK snapshot restoration",
        )
    if not db_sync_revision:
        pytest.skip(
            "--db-sync-revision required for IOHK snapshot restoration",
        )

    config = db_sync.create_db_sync_config(
        env=env,
        workdir=sync_context.workdir,
    )

    platform_system, platform_release, platform_version = helpers.get_os_type()
    start_test_time = datetime.datetime.now(
        tz=datetime.timezone.utc,
    ).strftime("%d/%m/%Y %H:%M:%S")

    snapshot_url = snapshot_url_opt or db_sync.get_latest_snapshot_url(env, None)
    LOGGER.info("Snapshot URL: %s", snapshot_url)

    # node setup
    conf_dir = pl.Path.cwd()
    base_dir = pl.Path.cwd()
    bin_dir = pl.Path("bin")
    bin_dir.mkdir(exist_ok=True)
    node.add_to_path(path=bin_dir)

    node.set_node_socket_path_env_var(base_dir=base_dir)
    node.get_node_files(node_rev=node_revision, base_dir=base_dir)
    cli_version, cli_git_rev = node.get_node_version()
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
    (
        node_sync_time_seconds,
        _last_slot_no,
        _latest_chunk_no,
        _era_details_dict,
        _epoch_details_dict,
    ) = node.wait_for_node_to_sync(env=env, base_dir=base_dir)

    # db-sync setup
    LOGGER.info("Building db-sync from revision: %s", db_sync_revision)
    db_sync_dir = gitpython.clone_repo(
        "cardano-db-sync",
        db_sync_revision,
    )
    db_sync.setup_postgres(config)
    db_sync.create_pgpass_file(config)
    db_sync.create_database()
    db_sync.list_databases(config)
    helpers.execute_command(
        "nix build -v --accept-flake-config --print-build-logs .#cardano-db-sync -o db-sync-node",
        cwd=db_sync_dir,
    )
    helpers.execute_command(
        "nix build -v --accept-flake-config --print-build-logs .#cardano-db-tool -o db-sync-tool",
        cwd=db_sync_dir,
    )
    db_sync.copy_db_sync_executables(config, build_method="nix")

    # download and verify snapshot
    LOGGER.info("Downloading snapshot")
    snapshot_name = db_sync.download_db_sync_snapshot(snapshot_url)
    expected_sha = db_sync.get_snapshot_sha_256_sum(snapshot_url)
    actual_sha = helpers.get_file_sha256_sum(snapshot_name)
    assert expected_sha == actual_sha, "Snapshot SHA-256 mismatch"

    # restore snapshot
    LOGGER.info("Restoring from IOHK snapshot")
    restoration_time = db_sync.restore_db_sync_from_snapshot(
        config,
        snapshot_name,
        remove_ledger_dir="no",
    )
    LOGGER.info("Restoration time: %d seconds", restoration_time)

    db_sync_tip = db_sync.get_db_sync_tip(config)
    if db_sync_tip is None:
        msg = "db-sync tip unavailable after snapshot restoration"
        raise RuntimeError(msg)
    snapshot_epoch_no = db_sync_tip.epoch_no
    snapshot_block_no = db_sync_tip.block_no
    snapshot_slot_no = db_sync_tip.slot_no

    # start db-sync
    LOGGER.info("Starting db-sync after IOHK snapshot restoration")
    db_sync.start_db_sync(config, start_args="", first_start="True")
    helpers.print_last_n_lines(config.db_sync_log_file, 30)
    db_sync_version, db_sync_git_rev = db_sync.get_db_sync_version(config)
    db_full_sync_time_in_secs, perf_stats = db_sync.wait_for_db_to_sync(
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
    helpers.print_last_n_lines(config.db_sync_log_file, 60)

    db_sync_tip = db_sync.get_db_sync_tip(config)
    if db_sync_tip is None:
        msg = "db-sync tip unavailable after post-restore sync wait"
        raise RuntimeError(msg)

    last_perf = db_sync.get_last_perf_stats_point(perf_stats)

    result: dict[str, tp.Any] = {
        "platform_system": platform_system,
        "platform_release": platform_release,
        "platform_version": platform_version,
        "env": env,
        "cli_version": cli_version,
        "cli_git_rev": cli_git_rev,
        "db_sync_version": db_sync_version,
        "db_sync_git_rev": db_sync_git_rev,
        "start_test_time": start_test_time,
        "end_test_time": end_test_time,
        "node_sync_time_seconds": node_sync_time_seconds,
        "db_full_sync_time_in_secs": db_full_sync_time_in_secs,
        "snapshot_url": snapshot_url,
        "snapshot_name": snapshot_name,
        "restoration_time": restoration_time,
        "snapshot_epoch_no": snapshot_epoch_no,
        "snapshot_block_no": snapshot_block_no,
        "snapshot_slot_no": snapshot_slot_no,
        "last_synced_epoch_no": db_sync_tip.epoch_no,
        "last_synced_block_no": db_sync_tip.block_no,
        "last_synced_slot_no": db_sync_tip.slot_no,
        "cpu_percent_usage": last_perf.cpu_percent_usage,
        "rss_mem_usage": last_perf.rss_mem_usage,
        "perf_stats": perf_stats,
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


class TestIohkSnapshotRestoration:
    """Validate db-sync restoration from an IOHK official snapshot."""

    def test_restoration_completed(
        self,
        iohk_restoration_result: dict[str, tp.Any],
    ):
        """Check that the IOHK snapshot was restored and db-sync synced."""
        assert iohk_restoration_result["restoration_time"] > 0
        assert iohk_restoration_result["db_full_sync_time_in_secs"] >= 0
        assert (
            iohk_restoration_result["last_synced_epoch_no"]
            >= (iohk_restoration_result["snapshot_epoch_no"])
        )

    def test_restoration_results_json(
        self,
        sync_context: SyncContext,  # noqa: ARG002
        iohk_restoration_result: dict[str, tp.Any],
    ):
        """Generate and upload IOHK snapshot restoration test results."""
        config = iohk_restoration_result["config"]
        test_results_file = config.workdir / "db_sync_iohk_snapshot_restoration_test_results.json"

        test_data: OrderedDict[str, tp.Any] = OrderedDict()
        test_data["platform_system"] = iohk_restoration_result["platform_system"]
        test_data["platform_release"] = iohk_restoration_result["platform_release"]
        test_data["platform_version"] = iohk_restoration_result["platform_version"]
        test_data["no_of_cpu_cores"] = os.cpu_count()
        test_data["total_ram_in_GB"] = helpers.get_total_ram_in_gb()
        test_data["env"] = iohk_restoration_result["env"]
        test_data["node_cli_version"] = iohk_restoration_result["cli_version"]
        test_data["node_git_revision"] = iohk_restoration_result["cli_git_rev"]
        test_data["db_sync_version"] = iohk_restoration_result["db_sync_version"]
        test_data["db_sync_git_rev"] = iohk_restoration_result["db_sync_git_rev"]
        test_data["start_test_time"] = iohk_restoration_result["start_test_time"]
        test_data["end_test_time"] = iohk_restoration_result["end_test_time"]
        test_data["node_total_sync_time_in_sec"] = iohk_restoration_result["node_sync_time_seconds"]
        test_data["node_total_sync_time_in_h_m_s"] = str(
            datetime.timedelta(
                seconds=int(iohk_restoration_result["node_sync_time_seconds"]),
            )
        )
        test_data["db_total_sync_time_in_sec"] = iohk_restoration_result[
            "db_full_sync_time_in_secs"
        ]
        test_data["db_total_sync_time_in_h_m_s"] = str(
            datetime.timedelta(
                seconds=iohk_restoration_result["db_full_sync_time_in_secs"],
            )
        )
        test_data["snapshot_url"] = iohk_restoration_result["snapshot_url"]
        test_data["snapshot_name"] = iohk_restoration_result["snapshot_name"]
        test_data["snapshot_epoch_no"] = iohk_restoration_result["snapshot_epoch_no"]
        test_data["snapshot_block_no"] = iohk_restoration_result["snapshot_block_no"]
        test_data["snapshot_slot_no"] = iohk_restoration_result["snapshot_slot_no"]
        test_data["last_synced_epoch_no"] = iohk_restoration_result["last_synced_epoch_no"]
        test_data["last_synced_block_no"] = iohk_restoration_result["last_synced_block_no"]
        test_data["last_synced_slot_no"] = iohk_restoration_result["last_synced_slot_no"]
        test_data["cpu_percent_usage"] = iohk_restoration_result["cpu_percent_usage"]
        test_data["total_rss_memory_usage_in_B"] = iohk_restoration_result["rss_mem_usage"]
        test_data["total_database_size"] = db_sync.get_total_db_size(
            config,
        )
        test_data["rollbacks"] = log_analyzer.are_rollbacks_present_in_logs(
            log_file=config.db_sync_log_file,
        )
        test_data["errors"] = log_analyzer.is_string_present_in_file(
            file_to_check=config.db_sync_log_file,
            search_string="db-sync-node:Error",
        )

        helpers.write_json_to_file(test_results_file, test_data)

        # write enriched perf stats
        era_activation = db_sync.get_era_activation_data(config)
        enriched_perf_stats = db_sync.enrich_perf_stats_with_era(
            iohk_restoration_result["perf_stats"],
            era_activation,
        )
        helpers.write_json_to_file(
            config.perf_stats_file,
            enriched_perf_stats,
        )
        db_sync.export_epoch_sync_times_from_db(
            config,
            config.epoch_sync_times_file,
            iohk_restoration_result["snapshot_epoch_no"],
        )

        # compress and upload artifacts
        helpers.zip_file(
            config.node_archive_name,
            config.node_log_file,
        )
        helpers.zip_file(
            config.db_sync_archive_name,
            config.db_sync_log_file,
        )
        helpers.zip_file(
            config.sync_data_archive_name,
            config.epoch_sync_times_file,
        )
        helpers.zip_file(
            config.perf_stats_archive_name,
            config.perf_stats_file,
        )

        db_sync.upload_artifact(config.node_archive_name)
        db_sync.upload_artifact(config.db_sync_archive_name)
        db_sync.upload_artifact(config.sync_data_archive_name)
        db_sync.upload_artifact(config.perf_stats_archive_name)
        db_sync.upload_artifact(str(test_results_file))

        assert test_results_file.exists()

    def test_restoration_log_analysis(
        self,
        iohk_restoration_result: dict[str, tp.Any],
    ):
        """Check db-sync logs for errors after IOHK snapshot restoration."""
        config = iohk_restoration_result["config"]
        log_analyzer.check_db_sync_logs(log_file=config.db_sync_log_file)
