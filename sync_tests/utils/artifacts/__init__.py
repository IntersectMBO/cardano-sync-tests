"""Artifact upload and management utilities for sync tests."""

from __future__ import annotations

import logging
import os
import pathlib as pl
import shutil

from sync_tests.scripts.sync_static_graphs import generate_static_graphs
from sync_tests.utils import helpers
from sync_tests.utils.db_sync.config import DbSyncConfig
from sync_tests.utils.db_sync.data import export_epoch_sync_times_from_db
from sync_tests.utils.db_sync.perf_utils import enrich_perf_stats_with_era

LOGGER = logging.getLogger(__name__)


def generate_result_graphs(
    workdir: pl.Path, results_files: list[pl.Path], mode: str = "auto"
) -> pl.Path:
    """Generate performance graphs from sync results JSON files into workdir/graphs.

    Best-effort: a failure here must not fail CI artifact bundling, it only
    means graphs are skipped for this run (same as if nobody had run
    sync_static_graphs.py at all).

    Args:
        workdir: Session working directory; graphs are written to workdir/graphs.
        results_files: Result JSON files to generate graphs from.
        mode: "node", "dbsync", or "auto" (majority-vote detection). Pass an
            explicit mode when results_files is known to be single-purpose,
            e.g. to generate node graphs even when a db-sync results file is
            also present in the same workdir.

    Returns:
        The graphs directory (may be empty if generation failed or produced nothing).
    """
    graphs_dir = workdir / "graphs"
    try:
        generate_static_graphs(
            file_list=[str(f) for f in results_files],
            output_dir=str(graphs_dir),
            dpi=150,
            fmt="png",
            mode=mode,
        )
    except Exception:
        LOGGER.exception("Failed to generate result graphs; continuing without them")
    return graphs_dir


def is_ci_environment() -> bool:
    """Check if we're running in a CI environment across supported providers."""
    return bool(
        os.getenv("CI")
        or os.getenv("GITHUB_ACTIONS")
        or os.getenv("GITLAB_CI")
        or os.getenv("CIRCLECI")
    )


def upload_artifact(file: str, destination: str = "auto", local_dir: pl.Path | None = None) -> None:
    """Keep an artifact available for local inspection or CI bundle collection.

    Args:
        file: Path to the artifact file.
        destination: Retained for call-site compatibility; direct CI uploads are
            handled by workflow artifact steps.
        local_dir: Optional directory to copy the file to for local collection.
    """
    _ = destination
    if local_dir:
        local_path = pl.Path(local_dir) / pl.Path(file).name
        # Only copy if the file is not already in the target location
        if pl.Path(file).resolve() != local_path.resolve():
            local_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(file, local_path)
            LOGGER.info("Saved artifact locally to %s.", local_path)
        else:
            LOGGER.info("Artifact already in target location: %s.", local_path)
    else:
        LOGGER.info("Artifact ready for collection: %s", file)


def create_node_database_archive(config: DbSyncConfig) -> pl.Path:
    """Create an archive of the Cardano node database for the specified environment.

    Args:
        config: A DbSyncConfig instance with paths.

    Returns:
        Path: Path to the created archive file.
    """
    node_dir = config.workdir / "cardano-node"
    node_db_dir = node_dir / "db"
    if not node_dir.exists():
        msg = f"Node directory not found for archive creation: {node_dir}"
        raise FileNotFoundError(msg)
    if not node_db_dir.exists():
        msg = f"Node DB directory not found for archive creation: {node_db_dir}"
        raise FileNotFoundError(msg)
    node_db_archive = node_dir / f"node-db-{config.env}.tar.gz"

    archive_base_name = str(node_db_archive.parent / node_db_archive.stem)
    shutil.make_archive(archive_base_name, "gztar", root_dir=str(node_dir), base_dir="db")

    return node_db_archive


def emergency_upload_artifacts(
    config: DbSyncConfig, perf_stats: list[dict], era_activation: list[dict] | None = None
) -> None:
    """Flush in-progress artifact data to disk only.

    Writes enriched perf stats and epoch sync times to disk. The JSON files are
    picked up by test_upload_ci_artifacts and included in sync_results.zip. No
    zips are created here, no CI uploads are performed, and no processes are
    terminated; teardown owns process lifecycle.

    Args:
        config: A DbSyncConfig instance with paths and settings.
        perf_stats: A list of performance statistics dictionaries.
        era_activation: Optional era activation metadata to enrich perf stats.
    """
    output_perf_stats = perf_stats
    if era_activation:
        output_perf_stats = enrich_perf_stats_with_era(perf_stats, era_activation)
    helpers.write_json_to_file(config.perf_stats_file, output_perf_stats)
    export_epoch_sync_times_from_db(config, config.epoch_sync_times_file)
