from __future__ import annotations

import logging
import os
import pathlib as pl
import shutil
import subprocess
import tarfile
import time
import typing as tp
from pathlib import Path

import requests
import xmltodict

from sync_tests.utils import db_sync
from sync_tests.utils import helpers

LOGGER = logging.getLogger(__name__)


def _get_repo_root() -> Path:
    """Get the repository root directory.

    This function uses __file__ to reliably find the repo root regardless of
    the current working directory (which may change due to os.chdir() calls).

    Returns:
        Path: The repository root directory.
    """
    # This file is at sync_tests/utils/snapshots.py
    # Go up 2 levels to get repo root
    return Path(__file__).parent.parent.parent


def _get_db_sync_dir() -> Path:
    """Get the cardano-db-sync directory path.

    The cardano-db-sync repository is cloned to the repo root, not to test_workdir.

    Returns:
        Path: The cardano-db-sync directory path.
    """
    return _get_repo_root() / "cardano-db-sync"


def download_and_extract_node_snapshot(env: str) -> None:
    """Download and extracts the Cardano node snapshot for the specified environment."""
    current_directory = os.getcwd()
    headers = {"User-Agent": "Mozilla/5.0"}
    if env == "mainnet":
        snapshot_url = "https://update-cardano-mainnet.iohk.io/cardano-node-state/db-mainnet.tar.gz"
    else:
        snapshot_url = ""  # no other environments are supported for now

    archive_name = f"db-{env}.tar.gz"

    LOGGER.info("Download node snapshot file:")
    LOGGER.info(f" - current_directory: {current_directory}")
    LOGGER.info(f" - download_url: {snapshot_url}")
    LOGGER.info(f" - archive name: {archive_name}")

    with requests.get(snapshot_url, headers=headers, stream=True, timeout=2800) as r:
        r.raise_for_status()
        with open(archive_name, "wb") as f:
            f.writelines(r.iter_content(chunk_size=8192))

    LOGGER.info(f" ------ listdir (before archive extraction): {os.listdir(current_directory)}")
    tf = tarfile.open(Path(current_directory) / archive_name)
    tf.extractall(Path(current_directory))
    os.rename(f"db-{env}", "db")
    helpers.delete_file(Path(current_directory) / archive_name)
    LOGGER.info(f" ------ listdir (after archive extraction): {os.listdir(current_directory)}")


def get_latest_snapshot_url(env: str, args: tp.Any) -> str:
    """Fetch the latest snapshot URL for the specified environment."""
    github_snapshot_url: str = helpers.get_arg_value(args=args, key="snapshot_url")
    if github_snapshot_url != "latest":
        return github_snapshot_url

    if env == "mainnet":
        general_snapshot_url = "https://update-cardano-mainnet.iohk.io/?list-type=2&delimiter=/&prefix=cardano-db-sync/&max-keys=50&cachestamp=459588"
    else:
        msg = "Snapshot are currently available only for mainnet environment"
        raise ValueError(msg)

    headers = {"Content-type": "application/json"}
    res_with_latest_db_sync_version = requests.get(general_snapshot_url, headers=headers)
    dict_with_latest_db_sync_version = xmltodict.parse(res_with_latest_db_sync_version.content)
    db_sync_latest_version_prefix = dict_with_latest_db_sync_version["ListBucketResult"][
        "CommonPrefixes"
    ]["Prefix"]

    if env == "mainnet":
        latest_snapshots_list_url = f"https://update-cardano-mainnet.iohk.io/?list-type=2&delimiter=/&prefix={db_sync_latest_version_prefix}&max-keys=50&cachestamp=462903"
    else:
        msg = "Snapshot are currently available only for mainnet environment"
        raise ValueError(msg)

    res_snapshots_list = requests.get(latest_snapshots_list_url, headers=headers)
    dict_snapshots_list = xmltodict.parse(res_snapshots_list.content)
    latest_snapshot = dict_snapshots_list["ListBucketResult"]["Contents"][-2]["Key"]

    if env == "mainnet":
        latest_snapshot_url = f"https://update-cardano-mainnet.iohk.io/{latest_snapshot}"
    else:
        msg = "Snapshot are currently available only for mainnet environment"
        raise ValueError(msg)

    return latest_snapshot_url


def download_db_sync_snapshot(snapshot_url: str) -> str:
    """Download the database synchronization snapshot from a given URL."""
    current_directory = os.getcwd()
    headers = {"User-Agent": "Mozilla/5.0"}
    archive_name = snapshot_url.split("/")[-1].strip()

    LOGGER.info("Download db-sync snapshot file:")
    LOGGER.info(f" - current_directory: {current_directory}")
    LOGGER.info(f" - download_url: {snapshot_url}")
    LOGGER.info(f" - archive name: {archive_name}")

    with requests.get(snapshot_url, headers=headers, stream=True, timeout=60 * 60) as r:
        r.raise_for_status()
        with open(archive_name, "wb") as f:
            f.writelines(r.iter_content(chunk_size=8192))
    return archive_name


def get_snapshot_sha_256_sum(snapshot_url: str) -> str | None:
    """Calculate the SHA-256 checksum of the downloaded snapshot."""
    snapshot_sha_256_sum_url = snapshot_url + ".sha256sum"
    for line in requests.get(snapshot_sha_256_sum_url):
        return line.decode("utf-8").split(" ")[0]
    return None


def restore_db_sync_from_snapshot(
    config: db_sync.DbSyncConfig, snapshot_file: str | Path, remove_ledger_dir: str = "yes"
) -> int:
    """Restore the Cardano DB Sync database from a snapshot.

    Args:
        config: A DbSyncConfig instance with paths and settings.
        snapshot_file: Path to the snapshot file to restore.
        remove_ledger_dir: Whether to remove the existing ledger directory (defaults to "yes").

    Returns:
        int: Restoration time in seconds.
    """
    db_sync_dir = _get_db_sync_dir()
    snapshot_path = (
        pl.Path(snapshot_file).resolve()
        if not isinstance(snapshot_file, Path)
        else snapshot_file.resolve()
    )

    if remove_ledger_dir == "yes":
        ledger_state_dir = db_sync_dir / "ledger-state" / config.env
        if ledger_state_dir.exists():
            shutil.rmtree(ledger_state_dir)

    ledger_dir = db_sync_dir / "ledger-state" / config.env
    ledger_dir.mkdir(parents=True, exist_ok=True)
    LOGGER.info(f"ledger_dir: {ledger_dir}")

    tmp_dir = db_sync_dir / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    helpers.export_env_var("TMPDIR", str(tmp_dir))

    pgpass_file = db_sync_dir / "config" / f"pgpass-{config.env}"
    helpers.export_env_var("PGPASSFILE", str(pgpass_file))
    helpers.export_env_var("ENVIRONMENT", config.env)
    helpers.export_env_var("RESTORE_RECREATE_DB", "N")
    start_restoration = time.perf_counter()

    script_path = db_sync_dir / "scripts" / "postgresql-setup.sh"
    p = subprocess.Popen(
        [
            str(script_path),
            "--restore-snapshot",
            str(snapshot_path),
            str(ledger_dir),
        ],
        cwd=str(db_sync_dir),
        stdout=subprocess.PIPE,
    )
    try:
        outs, errs = p.communicate(timeout=36000)
        output = outs.decode("utf-8")
        print(f"Restore database: {output}")
        if errs:
            errors = errs.decode("utf-8")
            LOGGER.error(f"Error during restoration: {errors}")

    except subprocess.CalledProcessError as e:
        msg = "command '{}' return with error (code {}): {}".format(
            e.cmd, e.returncode, " ".join(str(e.output).split())
        )
        raise RuntimeError(msg) from e
    except subprocess.TimeoutExpired:
        p.kill()
        LOGGER.exception("Process timeout expired")

    finally:
        helpers.export_env_var("TMPDIR", "/tmp")

    if "All good!" not in outs.decode("utf-8"):
        msg = "Restoration has not ended successfully"
        raise RuntimeError(msg)

    end_restoration = time.perf_counter()
    return int(end_restoration - start_restoration)


def create_db_sync_snapshot_stage_1(config: db_sync.DbSyncConfig) -> str:
    """Perform the first stage of creating a DB Sync snapshot.

    Args:
        config: A DbSyncConfig instance with paths and settings.

    Returns:
        str: The command to run for stage 2 of snapshot creation.
    """
    db_sync_dir = _get_db_sync_dir()
    db_tool_binary = db_sync_dir / "_cardano-db-tool"

    pgpass_file = db_sync_dir / "config" / f"pgpass-{config.env}"
    helpers.export_env_var("PGPASSFILE", str(pgpass_file))

    cmd = [str(db_tool_binary), "prepare-snapshot", "--state-dir", f"ledger-state/{config.env}"]
    p = subprocess.Popen(
        cmd,
        cwd=str(db_sync_dir),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    )

    try:
        outs, errs = p.communicate(timeout=300)
        if errs:
            LOGGER.error(f"Warnings or Errors: {errs}")
        final_line_with_script_cmd = outs.split("\n")[2].lstrip()
        LOGGER.info(f"Snapshot Creation - Stage 1 result: {final_line_with_script_cmd}")
    except subprocess.CalledProcessError as e:
        msg = "command '{}' return with error (code {}): {}".format(
            e.cmd, e.returncode, " ".join(str(e.output).split())
        )
        raise RuntimeError(msg) from e
    else:
        return final_line_with_script_cmd


def create_db_sync_snapshot_stage_2(config: db_sync.DbSyncConfig, stage_2_cmd: str) -> str:
    """Perform the second stage of creating a DB Sync snapshot.

    Args:
        config: A DbSyncConfig instance with paths and settings.
        stage_2_cmd: The command to run for stage 2 (generated by stage 1).

    Returns:
        str: Path to the created snapshot file.
    """
    db_sync_dir = _get_db_sync_dir()

    pgpass_file = db_sync_dir / "config" / f"pgpass-{config.env}"
    helpers.export_env_var("PGPASSFILE", str(pgpass_file))

    try:
        result = subprocess.run(
            stage_2_cmd,
            shell=True,
            cwd=str(db_sync_dir),
            capture_output=True,
            text=True,
            timeout=43200,
            check=False,
        )

        LOGGER.info(f"Snapshot Creation - Stage 2 Output:\n{result.stdout}")
        if result.stderr:
            LOGGER.error(f"Warnings or Errors:\n{result.stderr}")
        snapshot_line = next(
            (line for line in result.stdout.splitlines() if line.startswith("Created")),
            "Snapshot creation output not found.",
        )
        snapshot_path = (
            snapshot_line.split()[1] if "Created" in snapshot_line else "Snapshot path unknown"
        )
    except subprocess.TimeoutExpired as e:
        msg = "Snapshot creation timed out."
        raise RuntimeError(msg) from e
    except subprocess.CalledProcessError as e:
        msg = f"Command '{e.cmd}' failed with error: {e.stderr}"
        raise RuntimeError(msg) from e
    else:
        return snapshot_path
