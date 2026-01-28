from __future__ import annotations

import logging
import shutil
import subprocess
import typing as tp
from pathlib import Path

from sync_tests.utils import helpers
from sync_tests.utils.db_sync_config import DbSyncConfig
from sync_tests.utils.db_sync_data import export_epoch_sync_times_from_db

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
    """Check if we're running in a CI environment (Buildkite available)."""
    return is_buildkite_available()


def upload_artifact(file: str, destination: str = "auto", local_dir: Path | None = None) -> None:
    """Upload an artifact to Buildkite if available, otherwise save locally.

    Args:
        file: Path to the file to upload.
        destination: Upload destination ("buildkite", "auto"). Defaults to "auto".
        local_dir: Optional directory to save file locally if Buildkite is not available.
    """
    if destination in ("buildkite", "auto"):
        try:
            cmd = ["buildkite-agent", "artifact", "upload", f"{file}"]
            subprocess.run(cmd, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            LOGGER.warning("Buildkite agent not available.")
        else:
            LOGGER.info(f"Uploaded {file} to Buildkite.")
            return

    # If Buildkite not available and local_dir is provided, save locally
    if local_dir:
        local_path = Path(local_dir) / Path(file).name
        # Only copy if the file is not already in the target location
        if Path(file).resolve() != local_path.resolve():
            local_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(file, local_path)
            LOGGER.info(f"Saved artifact locally to {local_path} (no Buildkite available).")
        else:
            LOGGER.info(
                f"Artifact already in target location: {local_path} (no Buildkite available)."
            )
    else:
        LOGGER.warning(
            "Skipping artifact upload (no Buildkite agent available). "
            "Logs remain in test_workdir/ for local inspection."
        )


def create_node_database_archive(config: DbSyncConfig) -> Path:
    """Create an archive of the Cardano node database for the specified environment.

    Args:
        config: A DbSyncConfig instance with paths.

    Returns:
        Path: Path to the created archive file.
    """
    node_dir = config.workdir / "cardano-node"
    node_db_archive = node_dir / f"node-db-{config.env}.tar.gz"

    archive_base_name = str(node_db_archive.parent / node_db_archive.stem)
    shutil.make_archive(archive_base_name, "gztar", root_dir=str(node_dir), base_dir="db")

    return node_db_archive


def set_buildkite_meta_data(key: str, value: tp.Any) -> None:
    """Set metadata in Buildkite for the specified key and value."""
    p = subprocess.Popen(["buildkite-agent", "meta-data", "set", f"{key}", f"{value}"])
    p.communicate(timeout=15)


def get_buildkite_meta_data(key: str) -> str:
    """Retrieve metadata from Buildkite for the specified key."""
    p = subprocess.Popen(
        ["buildkite-agent", "meta-data", "get", f"{key}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    outs, _errs = p.communicate(timeout=15)
    return outs.decode("utf-8").strip()


def emergency_upload_artifacts(config: DbSyncConfig, perf_stats: list[dict]) -> None:
    """Upload artifacts for debugging in case of an emergency.

    Args:
        config: A DbSyncConfig instance with paths and settings.
        perf_stats: A list of performance statistics dictionaries.
    """
    helpers.write_json_to_file(config.perf_stats_file, perf_stats)
    export_epoch_sync_times_from_db(config, config.epoch_sync_times_file)

    helpers.zip_file(config.perf_stats_archive_name, config.perf_stats_file)
    helpers.zip_file(config.sync_data_archive_name, config.epoch_sync_times_file)
    helpers.zip_file(config.db_sync_archive_name, config.db_sync_log_file)
    helpers.zip_file(config.node_archive_name, config.node_log_file)

    upload_artifact(config.perf_stats_archive_name)
    upload_artifact(config.sync_data_archive_name)
    upload_artifact(config.db_sync_archive_name)
    upload_artifact(config.node_archive_name)

    helpers.manage_process(proc_name="cardano-db-sync", action="terminate")
    helpers.manage_process(proc_name="cardano-node", action="terminate")
