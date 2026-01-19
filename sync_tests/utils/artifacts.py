import logging
import shutil
import subprocess
import typing as tp
from pathlib import Path

from sync_tests.utils import db_sync
from sync_tests.utils import helpers

LOGGER = logging.getLogger(__name__)


def upload_artifact(file: str, destination: str = "auto", s3_path: str | None = None) -> None:
    """Upload an artifact to either S3 or Buildkite based on the specified destination."""
    if destination in ("buildkite", "auto"):
        try:
            cmd = ["buildkite-agent", "artifact", "upload", f"{file}"]
            subprocess.run(cmd, check=True)
            LOGGER.info(f"Uploaded {file} to Buildkite.")
        except (subprocess.CalledProcessError, FileNotFoundError):
            LOGGER.warning("Buildkite agent not available. Falling back to S3.")
        else:
            return

    if destination in ("s3", "auto"):
        if not s3_path:
            msg = "S3 path must be specified for S3 uploads."
            raise ValueError(msg)
        try:
            cmd = ["aws", "s3", "cp", f"{file}", f"s3://{s3_path}"]
            subprocess.run(cmd, check=True)
            LOGGER.info(f"Uploaded {file} to S3 at {s3_path}.")
        except subprocess.CalledProcessError:
            LOGGER.exception(f"Error uploading {file} to S3")


def create_node_database_archive(config: db_sync.DbSyncConfig) -> Path:
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


def emergency_upload_artifacts(config: db_sync.DbSyncConfig, perf_stats: list[dict]) -> None:
    """Upload artifacts for debugging in case of an emergency.

    Args:
        config: A DbSyncConfig instance with paths and settings.
        perf_stats: A list of performance statistics dictionaries.
    """
    from sync_tests.utils import postgres

    helpers.write_json_to_file(config.perf_stats_file, perf_stats)
    postgres.export_epoch_sync_times_from_db(config, config.epoch_sync_times_file)

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

