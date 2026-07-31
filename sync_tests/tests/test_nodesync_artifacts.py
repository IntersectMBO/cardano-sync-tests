"""Node sync validation and artifact generation tests."""

from __future__ import annotations

import datetime
import json
import logging
import os
import pathlib as pl
import typing as tp

import pytest

from sync_tests.tests.conftest import NodeSyncResult
from sync_tests.tests.conftest import SyncContext
from sync_tests.utils import artifacts
from sync_tests.utils import db_sync
from sync_tests.utils import helpers
from sync_tests.utils import node
from sync_tests.utils.logs import metrics_extractor as node_metrics

LOGGER = logging.getLogger(__name__)

pytestmark = pytest.mark.node_sync


def _epoch_duration_from_log_values(env: str, log_values: dict[str, tp.Any]) -> dict[int, int]:
    """Derive per-epoch durations from node ``log_values`` tip progression."""
    epoch_d_zero = node.get_epoch_no_d_zero(env=env)
    start_slot_d_zero = node.get_start_slot_no_d_zero(env=env)
    if epoch_d_zero is None or start_slot_d_zero is None:
        return {}

    try:
        epoch_length = node.get_shelley_slot_ln(conf_dir=pl.Path.cwd())
    except (FileNotFoundError, KeyError, ValueError):
        return {}
    if epoch_length <= 0:
        return {}

    samples: list[tuple[datetime.datetime, int]] = []
    for ts_key, sample in log_values.items():
        if not isinstance(sample, dict):
            continue
        slot_no = sample.get("tip")
        if slot_no is None:
            continue
        try:
            slot_no_int = int(slot_no)
            ts = datetime.datetime.fromisoformat(ts_key)
        except (TypeError, ValueError):
            continue
        if slot_no_int < start_slot_d_zero:
            continue
        samples.append((ts, slot_no_int))

    if len(samples) < 2:
        return {}
    samples.sort(key=lambda item: item[0])

    epoch_first_seen: dict[int, datetime.datetime] = {}
    epoch_last_seen: dict[int, datetime.datetime] = {}
    for ts, slot_no in samples:
        epoch_no = epoch_d_zero + ((slot_no - start_slot_d_zero) // epoch_length)
        epoch_first_seen.setdefault(epoch_no, ts)
        epoch_last_seen[epoch_no] = ts

    epoch_duration: dict[int, int] = {}
    for epoch_no, start_ts in epoch_first_seen.items():
        end_ts = epoch_last_seen.get(epoch_no, start_ts)
        duration = int((end_ts - start_ts).total_seconds())
        epoch_duration[epoch_no] = max(duration, 1)
    return epoch_duration


def _sync_time_from_log_values(log_values: dict[str, tp.Any]) -> int:
    """Compute wall-clock sync duration from first/last log sample timestamps."""
    timestamps: list[datetime.datetime] = []
    for ts_key in log_values:
        try:
            timestamps.append(datetime.datetime.fromisoformat(ts_key))
        except ValueError:
            continue
    if len(timestamps) < 2:
        return 0
    timestamps.sort()
    return max(int((timestamps[-1] - timestamps[0]).total_seconds()), 0)


class TestNodeSyncArtifacts:
    """Validate node sync and generate node-sync artifacts."""

    def test_node_reached_tip(
        self,
        sync_context: SyncContext,
        node_synced: NodeSyncResult | None,
    ):
        """Check that the node reports syncProgress == 100."""
        if node_synced is None:
            pytest.skip("node_synced is None; node sync was skipped")

        live_tip_unavailable = False
        try:
            live_tip = node.get_current_tip(env=sync_context.env)
            tip_data = {
                "epoch": live_tip.epoch,
                "block": live_tip.block,
                "slot": live_tip.slot,
                "era": live_tip.era,
                "sync_progress": live_tip.sync_progress,
            }
            sync_progress = live_tip.sync_progress
        except Exception:
            LOGGER.warning(
                "Node socket unavailable; falling back to recorded tip from node_synced",
                exc_info=True,
            )
            live_tip_unavailable = True
            tip_data = node_synced.node_tip
            sync_progress = node_synced.node_tip.get("sync_progress")

        LOGGER.info(
            "Live node tip: %s",
            json.dumps(tip_data, indent=2, default=str),
        )
        LOGGER.info(
            "Node sync window: %s -> %s",
            node_synced.node_start_time,
            node_synced.node_end_time,
        )
        LOGGER.info("Node marker: %s", node_synced.node_marker)

        assert sync_progress is not None, "syncProgress missing from node tip"
        if live_tip_unavailable and not node_synced.full_sync:
            pytest.skip(
                "Combined mode: live tip unavailable and recorded tip is from "
                "start-era checkpoint, not final sync state."
            )
        assert sync_progress >= 100, f"Node not fully synced: syncProgress={sync_progress}"

    def test_node_marker_written(
        self,
        sync_context: SyncContext,
        node_synced: NodeSyncResult | None,
    ):
        """Check that the node completion marker was written."""
        if node_synced is None:
            pytest.skip("node_synced is None; node sync was skipped")
        marker_file = sync_context.marker_file_path
        assert marker_file.exists(), f"Marker file not found: {marker_file}"
        with open(marker_file) as fh:
            data = json.load(fh)
        assert "node_synced" in data, "node_synced entry missing from marker status"
        assert data["node_synced"]["marker"] == "SYNC_MARKER_NODE_DONE"
        LOGGER.info("Node marker verified in %s", marker_file)

    def test_generate_node_sync_results(
        self,
        sync_context: SyncContext,
        node_synced: NodeSyncResult | None,
    ):
        """Generate node sync results JSON from node sync data."""
        if node_synced is None:
            pytest.skip("node_synced is None; node sync was skipped")

        env = sync_context.env
        results_file = sync_context.workdir / "node_sync_results.json"

        platform_system, platform_release, platform_version = helpers.get_os_type()

        epoch_details_secs: dict[tp.Any, tp.Any] = {
            epoch: data.get("sync_duration_secs", 0)
            for epoch, data in (node_synced.epoch_details or {}).items()
        }

        log_values: dict[str, tp.Any] = {}
        try:
            log_values = node_metrics.get_data_from_logs(
                sync_context.node_log_path,
            )
        except Exception:
            LOGGER.warning(
                "Failed to extract node log metrics",
                exc_info=True,
            )

        if not epoch_details_secs and log_values:
            epoch_details_secs = _epoch_duration_from_log_values(env=env, log_values=log_values)
            if epoch_details_secs:
                LOGGER.info(
                    "Derived node sync_duration_per_epoch from node logs (%d epochs)",
                    len(epoch_details_secs),
                )

        sync_time_seconds = node_synced.sync_time_sec
        if sync_time_seconds <= 0 and log_values:
            sync_time_seconds = _sync_time_from_log_values(log_values)
        if sync_time_seconds <= 0 and epoch_details_secs:
            sync_time_seconds = int(sum(epoch_details_secs.values()))

        last_slot_no = node_synced.last_slot_no
        if last_slot_no <= 0:
            slots = [
                int(sample.get("tip", 0))
                for sample in log_values.values()
                if isinstance(sample, dict) and sample.get("tip") is not None
            ]
            if slots:
                last_slot_no = max(slots)

        eras_in_test = list((node_synced.era_details or {}).keys())
        if not eras_in_test and node_synced.node_tip.get("era"):
            eras_in_test = [str(node_synced.node_tip.get("era")).lower()]

        test_values: dict[str, tp.Any] = {
            "env": env,
            "tag_no1": node_synced.node_revision,
            "tag_no2": None,
            "cli_version1": node_synced.cli_version,
            "cli_version2": None,
            "cli_git_rev1": node_synced.cli_git_rev,
            "cli_git_rev2": None,
            "cli_revision1": node_synced.cli_version,
            "cli_revision2": None,
            "start_sync_time1": node_synced.node_start_time,
            "end_sync_time1": node_synced.node_end_time,
            "start_sync_time2": None,
            "end_sync_time2": None,
            "last_slot_no1": last_slot_no,
            "last_slot_no2": None,
            "start_node_secs1": node_synced.secs_to_start,
            "start_node_secs2": None,
            "sync_time_seconds1": sync_time_seconds,
            "sync_time1": str(
                datetime.timedelta(
                    seconds=int(sync_time_seconds),
                )
            ),
            "sync_time_seconds2": None,
            "sync_time2": None,
            "total_chunks1": node_synced.latest_chunk_no,
            "total_chunks2": None,
            "platform_system": platform_system,
            "platform_release": platform_release,
            "platform_version": platform_version,
            "chain_size_bytes": node_synced.chain_size_bytes,
            "sync_duration_per_epoch": epoch_details_secs,
            "eras_in_test": eras_in_test,
            "no_of_cpu_cores": os.cpu_count(),
            "total_ram_in_GB": helpers.get_total_ram_in_gb(),
            "epoch_no_d_zero": node.get_epoch_no_d_zero(env=env),
            "start_slot_no_d_zero": (node.get_start_slot_no_d_zero(env=env)),
            "hydra_eval_no1": node_synced.node_revision,
            "hydra_eval_no2": None,
            "node_revision1": node_synced.node_revision,
            "node_revision2": None,
            "log_values": log_values,
        }

        for era, era_data in (node_synced.era_details or {}).items():
            test_values.update(
                {
                    f"{era}_start_time": era_data.get("start_time"),
                    f"{era}_start_epoch": era_data.get("start_epoch"),
                    f"{era}_slots_in_era": era_data.get("slots_in_era"),
                    f"{era}_start_sync_time": era_data.get(
                        "start_sync_time",
                    ),
                    f"{era}_end_sync_time": era_data.get("end_sync_time"),
                    f"{era}_sync_duration_secs": era_data.get(
                        "sync_duration_secs",
                    ),
                    f"{era}_sync_speed_sps": era_data.get("sync_speed_sps"),
                }
            )

        if eras_in_test and sync_time_seconds > 0:
            primary_era = eras_in_test[-1]
            test_values.setdefault(f"{primary_era}_sync_duration_secs", sync_time_seconds)

        helpers.write_json_to_file(results_file, test_values)
        LOGGER.info("node sync results written to %s", results_file)

        assert results_file.exists(), f"Node sync results not created: {results_file}"
        with open(results_file) as fh:
            data = json.load(fh)
        assert data.get("env") == env
        assert data.get("sync_time_seconds1") is not None

        combined_mode = (sync_context.workdir / "db_sync_results.json").exists()
        if artifacts.is_ci_environment() and combined_mode:
            if len(data.get("log_values") or {}) < 10:
                LOGGER.warning(
                    "Combined CI run produced sparse node log_values (%s); "
                    "node graphs may be empty.",
                    len(data.get("log_values") or {}),
                )
            assert int(data.get("sync_time_seconds1") or 0) > 0, (
                "Combined CI run produced non-positive node sync_time_seconds1."
            )
            if len(data.get("sync_duration_per_epoch") or {}) == 0:
                LOGGER.warning(
                    "Combined CI run produced empty sync_duration_per_epoch; "
                    "epoch-duration graph will be skipped.",
                )

    def test_prepare_ci_artifacts(
        self,
        sync_context: SyncContext,
        node_synced: NodeSyncResult | None,  # noqa: ARG002
    ):
        """Prepare node-only CI bundles for artifact collection.

        Creates standardized logs/results bundles that any CI system can collect.
        Skips in local runs and skips in combined runs where db-sync owns bundling.
        """
        if not artifacts.is_ci_environment():
            pytest.skip("Not in CI environment; skipping bundle generation")

        workdir = sync_context.workdir
        db_sync_results = workdir / "db_sync_results.json"
        if db_sync_results.exists():
            LOGGER.info(
                "DB-sync results found; skipping node-only bundling. "
                "DB-sync artifact test will prepare CI bundles."
            )
            pytest.skip("DB-sync bundling will include all artifacts")

        root_dir = pl.Path.cwd()

        # Stop the monitor now so its EXIT trap fires and writes oom.log before
        # we build monitor.zip. The fixture teardown stop_monitor() call becomes
        # a no-op (pid file already gone).
        db_sync.stop_monitor(workdir)

        log_files = helpers.list_sync_log_files(workdir)
        results_files = sorted(workdir.glob("*.json"))
        results_file = workdir / "node_sync_results.json"

        # Generate graphs from the results we just wrote, then include them
        graphs_dir = artifacts.generate_result_graphs(workdir, [results_file], mode="node")
        graph_files = sorted(graphs_dir.glob("*.png")) if graphs_dir.exists() else []

        assert log_files, f"No log files found for CI bundling in {workdir}"
        assert results_files, f"No JSON files found for CI bundling in {workdir}"
        assert results_file.exists(), f"Missing node-sync results JSON: {results_file}"

        logs_bundle = root_dir / "sync_logs.zip"
        results_bundle = root_dir / "sync_results.zip"
        monitor_bundle = root_dir / "monitor.zip"
        helpers.create_zip_bundle(logs_bundle, log_files, base_dir=workdir)
        helpers.create_zip_bundle(results_bundle, results_files + graph_files, base_dir=workdir)
        helpers.zip_files(
            str(monitor_bundle),
            [
                workdir / "monitor.log",
                workdir / "monitor_stderr.log",
                workdir / "oom.log",
            ],
        )

        assert logs_bundle.exists(), f"Logs bundle was not created: {logs_bundle}"
        assert results_bundle.exists(), f"Results bundle was not created: {results_bundle}"
        monitor_status = monitor_bundle if monitor_bundle.exists() else "(monitor bundle skipped)"
        LOGGER.info(
            "Prepared node-only CI bundles for artifact_paths upload: %s, %s, %s",
            logs_bundle,
            results_bundle,
            monitor_status,
        )
