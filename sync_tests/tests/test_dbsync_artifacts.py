"""DB-sync artifact generation, CI upload, and log analysis."""

from __future__ import annotations

import datetime
import json
import logging
import os
import pathlib as pl
import typing as tp
from collections import OrderedDict

import pytest

from sync_tests.tests.conftest import DbSyncResult
from sync_tests.tests.conftest import NodeSyncResult
from sync_tests.tests.conftest import SyncContext
from sync_tests.utils import artifacts
from sync_tests.utils import db_sync
from sync_tests.utils import helpers
from sync_tests.utils.db_sync import metrics_extractor as db_sync_metrics_extractor
from sync_tests.utils.logs import log_analyzer

LOGGER = logging.getLogger(__name__)

pytestmark = pytest.mark.db_sync


class TestDbSyncArtifacts:
    """Generate DB-sync results JSON, upload artifacts, and analyze logs."""

    def test_generate_dbsync_results(
        self,
        sync_context: SyncContext,
        node_synced: NodeSyncResult | None,
        db_sync_synced: DbSyncResult,
    ):
        """Generate comprehensive test results JSON file."""
        env = sync_context.env
        config = db_sync.create_db_sync_config(
            env=env,
            workdir=sync_context.workdir,
        )

        test_results_file = config.workdir / "db_sync_results.json"

        platform_system, platform_release, platform_version = helpers.get_os_type()

        test_data: OrderedDict[str, tp.Any] = OrderedDict()

        test_data["platform_system"] = platform_system
        test_data["platform_release"] = platform_release
        test_data["platform_version"] = platform_version
        test_data["no_of_cpu_cores"] = os.cpu_count()
        test_data["total_ram_in_GB"] = helpers.get_total_ram_in_gb()
        test_data["env"] = env

        test_data["node_pr"] = config.node_pr or ""
        test_data["node_branch"] = config.node_branch or ""
        test_data["node_version"] = node_synced.node_revision if node_synced else None
        test_data["db_sync_branch"] = config.db_sync_branch or ""

        test_data["db_sync_revision"] = db_sync_synced.db_sync_revision or ""
        test_data["db_sync_start_options"] = db_sync_synced.db_start_options or ""
        test_data["node_cli_version"] = node_synced.cli_version if node_synced else None
        test_data["node_git_revision"] = node_synced.cli_git_rev if node_synced else None
        test_data["db_sync_version"] = db_sync_synced.db_sync_version
        test_data["db_sync_git_rev"] = db_sync_synced.db_sync_git_rev

        test_data["start_test_time"] = db_sync_synced.db_start_time
        test_data["end_test_time"] = db_sync_synced.db_end_time
        sync_secs = db_sync_synced.db_full_sync_time_in_secs or 0
        test_data["total_sync_time_in_sec"] = sync_secs
        test_data["total_sync_time_in_h_m_s"] = str(datetime.timedelta(seconds=int(sync_secs)))

        tip = db_sync_synced.db_tip
        test_data["last_synced_epoch_no"] = tip.get("epoch_no")
        test_data["last_synced_block_no"] = tip.get("block_no")
        test_data["last_synced_slot_no"] = tip.get("slot_no")

        perf_stats = db_sync_synced.perf_stats or []
        last_point = db_sync.get_last_perf_stats_point(
            perf_stats,
        )
        test_data["cpu_percent_usage"] = last_point.cpu_percent_usage
        test_data["total_rss_memory_usage_in_B"] = last_point.rss_mem_usage

        try:
            test_data["total_database_size"] = db_sync.get_total_db_size(config)
        except (OSError, RuntimeError, TimeoutError, ValueError):
            LOGGER.warning(
                "Failed to get database size",
                exc_info=True,
            )
            test_data["total_database_size"] = None

        db_log = sync_context.db_sync_log_path
        test_data["rollbacks"] = log_analyzer.are_rollbacks_present_in_logs(
            log_file=db_log,
        )
        test_data["errors"] = log_analyzer.is_string_present_in_file(
            file_to_check=db_log,
            search_string="db-sync-node:Error",
        )

        enriched_path = db_sync_synced.perf_stats_path
        if enriched_path and enriched_path.exists():
            with open(enriched_path) as fh:
                enriched_stats = json.load(fh)
            test_data["system_metrics"] = enriched_stats
        else:
            test_data["system_metrics"] = perf_stats
        _extract_log_metrics(test_data, db_log)

        helpers.write_json_to_file(test_results_file, test_data)
        LOGGER.info(
            "Test results written to: %s",
            test_results_file,
        )

        assert test_results_file.exists(), f"Test results file not created: {test_results_file}"
        with open(test_results_file) as fh:
            data = json.load(fh)
        assert data.get("env") == env
        assert data.get("total_sync_time_in_sec") is not None

    def test_upload_ci_artifacts(
        self,
        sync_context: SyncContext,
        node_synced: NodeSyncResult | None,  # noqa: ARG002
        db_sync_synced: DbSyncResult,  # noqa: ARG002
    ):
        """Prepare comprehensive CI bundles for artifact collection.

        Creates standardized logs/results bundles that any CI system can collect.
        Upload remains CI-runner responsibility.
        """
        if not artifacts.is_ci_environment():
            LOGGER.info(
                "Not in CI - logs remain in %s",
                sync_context.workdir,
            )
            pytest.skip(
                "Not in CI environment; skipping upload",
            )

        env = sync_context.env
        config = db_sync.create_db_sync_config(
            env=env,
            workdir=sync_context.workdir,
        )

        # Stop the monitor now so its EXIT trap fires and writes oom.log before
        # we build monitor.zip. The fixture teardown stop_monitor() call becomes
        # a no-op (pid file already gone).
        db_sync.stop_monitor(sync_context.workdir)

        root_dir = pl.Path.cwd()
        test_results_file = config.workdir / "db_sync_results.json"
        # Enforce CI artifact contract: publish exactly two bundles.
        log_files = helpers.list_sync_log_files(config.workdir)
        results_files = sorted(config.workdir.glob("*.json")) + sorted(
            (config.workdir / "cardano-db-sync").glob("*.json")
        )

        # Include graphs if they exist
        graphs_dir = config.workdir / "graphs"
        graph_files = sorted(graphs_dir.glob("*.png")) if graphs_dir.exists() else []

        assert log_files, f"No log files found for CI bundling in {config.workdir}"
        assert test_results_file.exists(), f"Missing db-sync results JSON: {test_results_file}"
        assert results_files, f"No JSON result files found for CI bundling in {config.workdir}"

        logs_bundle = root_dir / "sync_logs.zip"
        results_bundle = root_dir / "sync_results.zip"
        monitor_bundle = root_dir / "monitor.zip"

        helpers.create_zip_bundle(logs_bundle, log_files, base_dir=config.workdir)
        helpers.create_zip_bundle(
            results_bundle, results_files + graph_files, base_dir=config.workdir
        )
        helpers.zip_files(
            str(monitor_bundle),
            [
                config.monitor_log_file,
                config.monitor_stderr_log_file,
                config.oom_log_file,
                config.postgres_log_file,
            ],
        )

        assert logs_bundle.exists(), f"Logs bundle was not created: {logs_bundle}"
        assert results_bundle.exists(), f"Results bundle was not created: {results_bundle}"
        monitor_status = monitor_bundle if monitor_bundle.exists() else "(monitor bundle skipped)"
        LOGGER.info(
            "Prepared CI bundles for artifact_paths upload: %s, %s, %s",
            logs_bundle,
            results_bundle,
            monitor_status,
        )

    def test_log_analysis(
        self,
        sync_context: SyncContext,
        db_sync_synced: DbSyncResult,  # noqa: ARG002
    ):
        """Check db-sync logs for errors, rollbacks, and issues."""
        log_file = sync_context.db_sync_log_path
        assert log_file.exists(), f"DB sync log file not found: {log_file}"

        LOGGER.info("Analyzing db-sync logs: %s", log_file)
        log_analyzer.check_db_sync_logs(log_file=log_file)

        has_errors = log_analyzer.is_string_present_in_file(
            file_to_check=log_file,
            search_string="db-sync-node:Error",
        )
        has_rollbacks = log_analyzer.are_rollbacks_present_in_logs(
            log_file=log_file,
        )
        has_failed_rollbacks = log_analyzer.is_string_present_in_file(
            file_to_check=log_file,
            search_string="Rollback failed",
        )

        if has_errors:
            LOGGER.warning("Errors found in db-sync logs")
        if has_rollbacks:
            LOGGER.warning("Rollbacks found in db-sync logs")
        if has_failed_rollbacks:
            LOGGER.warning(
                "Failed rollbacks found in db-sync logs",
            )

        LOGGER.info("Log analysis complete")


def _extract_log_metrics(
    test_data: OrderedDict[str, tp.Any],
    db_log: pl.Path,
) -> None:
    """Extract epoch timing and block insertion metrics from db-sync logs.

    Args:
        test_data: Ordered dict to populate with ``epoch_timings``,
            ``block_insertion_rates``, and ``epoch_details``.
        db_log: Path to the db-sync log file.
    """
    try:
        log_metrics = db_sync_metrics_extractor.get_db_sync_data_from_logs(
            db_log,
        )
        test_data["epoch_timings"] = log_metrics["epoch_timings"]
        test_data["block_insertion_rates"] = log_metrics["block_insertions"]
        test_data["epoch_details"] = log_metrics["epoch_details"]
        LOGGER.info(
            "Extracted metrics for %d epochs",
            len(log_metrics["epoch_timings"]),
        )
    except (OSError, RuntimeError, ValueError):
        LOGGER.warning(
            "Failed to extract log-based metrics",
            exc_info=True,
        )
        test_data["epoch_timings"] = {}
        test_data["block_insertion_rates"] = []
        test_data["epoch_details"] = {}
