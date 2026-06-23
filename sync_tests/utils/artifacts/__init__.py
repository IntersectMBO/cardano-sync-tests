"""Artifact upload and management utilities for sync tests."""

from __future__ import annotations

import logging
import os
import pathlib as pl
import shutil
import subprocess
import typing as tp

from sync_tests.utils import helpers
from sync_tests.utils.db_sync.config import DbSyncConfig
from sync_tests.utils.db_sync.data import export_epoch_sync_times_from_db
from sync_tests.utils.db_sync.perf_utils import enrich_perf_stats_with_era

LOGGER = logging.getLogger(__name__)


def is_buildkite_available() -> bool:
    """Check if Buildkite agent is available."""
    try:
        subprocess.run(
            ["buildkite-agent", "--version"],
            capture_output=True,
            check=True,
            timeout=5,
        )
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return False
    return True


def is_ci_environment() -> bool:
    """Check if we're running in a CI environment across supported providers."""
    return bool(
        os.getenv("CI")
        or os.getenv("GITHUB_ACTIONS")
        or os.getenv("BUILDKITE")
        or os.getenv("GITLAB_CI")
        or os.getenv("CIRCLECI")
    )


def upload_artifact(file: str, destination: str = "auto", local_dir: pl.Path | None = None) -> None:
    """Upload an artifact to Buildkite if available, otherwise save locally.

    Args:
        file: Path to the file to upload.
        destination: Upload destination ("buildkite", "auto"). Defaults to "auto".
        local_dir: Optional directory to save file locally if Buildkite is not available.
    """
    if destination in ("buildkite", "auto"):
        try:
            cmd = ["buildkite-agent", "artifact", "upload", f"{file}"]
            subprocess.run(cmd, check=True, timeout=120)
        except FileNotFoundError:
            LOGGER.warning("Buildkite agent not available.")
        except subprocess.TimeoutExpired:
            LOGGER.warning("Timed out uploading artifact to Buildkite: %s", file)
        except subprocess.CalledProcessError as exc:
            LOGGER.warning(
                "Buildkite artifact upload failed (exit %s) for %s",
                exc.returncode,
                file,
            )
        else:
            LOGGER.info("Uploaded %s to Buildkite.", file)
            return

    # If Buildkite not available and local_dir is provided, save locally
    if local_dir:
        local_path = pl.Path(local_dir) / pl.Path(file).name
        # Only copy if the file is not already in the target location
        if pl.Path(file).resolve() != local_path.resolve():
            local_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(file, local_path)
            LOGGER.info("Saved artifact locally to %s (no Buildkite available).", local_path)
        else:
            LOGGER.info(
                "Artifact already in target location: %s (no Buildkite available).", local_path
            )
    else:
        LOGGER.warning(
            "Skipping artifact upload (no Buildkite agent available). "
            "Logs remain in test_workdir/ for local inspection."
        )


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


def set_buildkite_meta_data(key: str, value: tp.Any) -> None:
    """Set metadata in Buildkite for the specified key and value."""
    cmd = ["buildkite-agent", "meta-data", "set", f"{key}", f"{value}"]
    subprocess.run(cmd, check=True, timeout=15)


def get_buildkite_meta_data(key: str) -> str:
    """Retrieve metadata from Buildkite for the specified key."""
    result = subprocess.run(
        ["buildkite-agent", "meta-data", "get", f"{key}"],
        capture_output=True,
        text=True,
        timeout=15,
        check=True,
    )
    return result.stdout.strip()


def emergency_upload_artifacts(
    config: DbSyncConfig, perf_stats: list[dict], era_activation: list[dict] | None = None
) -> None:
    """Flush in-progress artifact data to disk only.

    Writes enriched perf stats and epoch sync times to disk. The JSON files are
    picked up by test_upload_ci_artifacts and included in sync_results.zip. No
    zips are created here, no Buildkite uploads are performed, and no processes
    are terminated — artifact collection is via artifact_paths only; teardown
    owns process lifecycle.

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
