"""Artifact upload and management utilities for sync tests."""

from __future__ import annotations

import logging
import os
import pathlib as pl
import shutil

from sync_tests.utils import helpers
from sync_tests.utils.db_sync.config import DbSyncConfig
from sync_tests.utils.db_sync.data import export_epoch_sync_times_from_db
from sync_tests.utils.db_sync.perf_utils import enrich_perf_stats_with_era

LOGGER = logging.getLogger(__name__)


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
