import dataclasses
import json
import logging
import os
import pathlib as pl
import platform
import shutil
import subprocess
import sys
import tarfile
import time
import typing as tp
from datetime import timedelta
from pathlib import Path

import matplotlib.pyplot as plt
import psutil
import psycopg2
import requests
import xmltodict
from assertpy import assert_that

from sync_tests.utils import helpers
from sync_tests.utils import node

LOGGER = logging.getLogger(__name__)


ONE_MINUTE = 60


@dataclasses.dataclass(frozen=True)
class DbSyncTip:
    """Database sync tip information."""

    epoch_no: int
    block_no: int
    slot_no: int


@dataclasses.dataclass(frozen=True)
class PerfStats:
    """Performance statistics for DB sync monitoring."""

    time: int
    slot_no: int
    cpu_percent_usage: float
    rss_mem_usage: int


@dataclasses.dataclass(frozen=True)
class DbSyncConfig:
    """Configuration for DB Sync operations."""

    # Environment
    env: str

    # Work directory
    workdir: Path

    # PostgreSQL configuration
    pg_host: str
    pg_port: str
    pg_user: str
    pg_dbname: str
    pg_dir: Path

    # Paths
    perf_stats_file: Path
    node_log_file: Path
    db_sync_log_file: Path
    epoch_sync_times_file: Path

    # Archive names
    node_archive_name: str
    db_sync_archive_name: str
    sync_data_archive_name: str
    perf_stats_archive_name: str

    # Chart
    chart_name: str

    # Optional environment variables (for reference)
    node_pr: str | None = None
    node_branch: str | None = None
    node_version: str | None = None
    db_sync_branch: str | None = None
    db_sync_version: str | None = None


def create_db_sync_config(
    env: str,
    workdir: Path | None = None,
    pg_host: str = "localhost",
    pg_port: str = "5432",
    pg_user: str | None = None,
    pg_dbname: str | None = None,
    pg_dir: Path | None = None,
) -> DbSyncConfig:
    """Create a DbSyncConfig instance from environment variables and parameters.

    Args:
        env: Environment name (e.g., "preview", "preprod", "mainnet").
        workdir: Working directory path (defaults to current directory).
        pg_host: PostgreSQL host (defaults to "localhost").
        pg_port: PostgreSQL port (defaults to "5432").
        pg_user: PostgreSQL user (defaults to current system user).
        pg_dbname: PostgreSQL database name (defaults to env name).
        pg_dir: PostgreSQL data directory (defaults to workdir.parent).

    Returns:
        DbSyncConfig: A configuration instance with all paths and settings.
    """
    if workdir is None:
        workdir = Path.cwd()
    workdir = workdir.resolve()

    if pg_user is None:
        pg_user = (
            subprocess.run(["whoami"], stdout=subprocess.PIPE, check=False)
            .stdout.decode("utf-8")
            .strip()
        )

    if pg_dbname is None:
        pg_dbname = env

    if pg_dir is None:
        pg_dir = workdir.parent

    # Build all paths relative to workdir
    perf_stats_file = workdir / f"cardano-db-sync/db_sync_{env}_performance_stats.json"
    node_log_file = workdir / f"cardano-node/node_{env}_logfile.log"
    db_sync_log_file = workdir / f"cardano-db-sync/db_sync_{env}_logfile.log"
    epoch_sync_times_file = workdir / f"cardano-db-sync/epoch_sync_times_{env}_dump.json"

    # Archive names
    node_archive_name = f"cardano_node_{env}_logs.zip"
    db_sync_archive_name = f"cardano_db_sync_{env}_logs.zip"
    sync_data_archive_name = f"epoch_sync_times_{env}_dump.zip"
    perf_stats_archive_name = f"db_sync_{env}_perf_stats.zip"

    # Chart name
    chart_name = f"full_sync_{env}_stats_chart.png"

    # Optional environment variables (for reference)
    node_pr = os.getenv("node_pr")
    node_branch = os.getenv("node_branch")
    node_version = os.getenv("node_version")
    db_sync_branch = os.getenv("db_sync_branch")
    db_sync_version = os.getenv("db_sync_version")

    return DbSyncConfig(
        env=env,
        workdir=workdir,
        pg_host=pg_host,
        pg_port=pg_port,
        pg_user=pg_user,
        pg_dbname=pg_dbname,
        pg_dir=pg_dir,
        perf_stats_file=perf_stats_file,
        node_log_file=node_log_file,
        db_sync_log_file=db_sync_log_file,
        epoch_sync_times_file=epoch_sync_times_file,
        node_archive_name=node_archive_name,
        db_sync_archive_name=db_sync_archive_name,
        sync_data_archive_name=sync_data_archive_name,
        perf_stats_archive_name=perf_stats_archive_name,
        chart_name=chart_name,
        node_pr=node_pr,
        node_branch=node_branch,
        node_version=node_version,
        db_sync_branch=db_sync_branch,
        db_sync_version=db_sync_version,
    )


def get_machine_name() -> str:
    """Retrieve the name of the machine."""
    return platform.node()


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


def print_file(file: str | Path, number_of_lines: int = 0) -> None:
    """Print contents of a file to the log, optionally limiting to a specified number of lines."""
    with open(file) as f:
        lines = f.readlines()
    for line in lines[-number_of_lines:] if number_of_lines else lines:
        LOGGER.info(line.strip())


def get_last_perf_stats_point(perf_stats: list[dict]) -> PerfStats:
    """Retrieve the last performance statistics data point, or initializes one if none exists.

    Args:
        perf_stats: A list of performance statistics dictionaries.

    Returns:
        PerfStats: The last performance statistics point, or a default one if none exists.
    """
    try:
        last_perf_stats_dict = perf_stats[-1]
        return PerfStats(**last_perf_stats_dict)
    except Exception:
        LOGGER.exception("Exception in get_last_perf_stats_point")
        default_stats = PerfStats(
            time=0,
            slot_no=0,
            cpu_percent_usage=0.0,
            rss_mem_usage=0,
        )
        perf_stats.append(dataclasses.asdict(default_stats))
        return default_stats


def get_log_output_frequency(env: str) -> int:
    """Determine the log output frequency based on the environment."""
    if env == "mainnet":
        return 20
    return 3


def export_epoch_sync_times_from_db(
    config: DbSyncConfig, file: str | Path, snapshot_epoch_no: int | str = 0
) -> str | None:
    """Export epoch synchronization times from the database to a file.

    Args:
        config: A DbSyncConfig instance with database connection settings.
        file: Path to the output file.
        snapshot_epoch_no: Minimum epoch number to export (defaults to 0).

    Returns:
        str: Output from psql command, or None on error.
    """
    db_sync_dir = config.workdir / "cardano-db-sync"
    output_file = pl.Path(file).resolve() if not isinstance(file, Path) else file.resolve()
    try:
        p = subprocess.Popen(
            [
                "psql",
                config.pg_dbname,
                "-t",
                "-c",
                rf"\o {output_file}",
                "-c",
                "SELECT array_to_json(array_agg(epoch_sync_time), FALSE) FROM "
                f"epoch_sync_time where no >= {snapshot_epoch_no};",
            ],
            cwd=str(db_sync_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        outs, errs = p.communicate(timeout=600)
        out = outs.decode("utf-8").strip() if outs else ""
        err = errs.decode("utf-8").strip() if errs else ""
        if err:
            LOGGER.error(
                f"Error during exporting epoch sync times from db: {err}. "
                "Killing extraction process."
            )
            p.kill()
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        p.kill()
        LOGGER.exception(
            "Error during exporting epoch sync times from db. Killing extraction process."
        )
    except Exception:
        LOGGER.exception(
            "Error during exporting epoch sync times from db. Killing extraction process."
        )
        p.kill()
    else:
        return out

    return None


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


def set_node_socket_path_env_var_in_cwd(config: DbSyncConfig) -> None:
    """Set the node socket path environment variable in the current working directory.

    Args:
        config: A DbSyncConfig instance with paths.
    """
    node_dir = config.workdir / "cardano-node"
    socket_path = node_dir / "db" / "node.socket"
    helpers.export_env_var("CARDANO_NODE_SOCKET_PATH", str(socket_path))


def create_pgpass_file(config: DbSyncConfig) -> None:
    """Create a PostgreSQL password file for the specified environment.

    Args:
        config: A DbSyncConfig instance with paths and PostgreSQL settings.
    """
    db_sync_config_dir = config.workdir / "cardano-db-sync" / "config"
    db_sync_config_dir.mkdir(parents=True, exist_ok=True)

    pgpass_file = db_sync_config_dir / f"pgpass-{config.env}"
    postgres_port = os.getenv("PGPORT", config.pg_port)
    pgpass_content = f"{config.pg_dir}:{postgres_port}:{config.pg_dbname}:{config.pg_user}:*"
    helpers.export_env_var("PGPASSFILE", str(pgpass_file))

    with open(pgpass_file, "w") as pgpass_text_file:
        print(pgpass_content, file=pgpass_text_file)
    os.chmod(pgpass_file, 0o600)


def create_database(config: DbSyncConfig) -> None:
    """Set up the PostgreSQL database for use with Cardano DB Sync.

    Args:
        config: A DbSyncConfig instance with paths and settings.
    """
    db_sync_dir = config.workdir / "cardano-db-sync"
    script_path = db_sync_dir / "scripts" / "postgresql-setup.sh"

    try:
        cmd = [str(script_path), "--createdb"]
        output = (
            subprocess.check_output(cmd, cwd=str(db_sync_dir), stderr=subprocess.STDOUT)
            .decode("utf-8")
            .strip()
        )
        LOGGER.info(f"Create database script output: {output}")
    except subprocess.CalledProcessError as e:
        msg = "command '{}' return with error (code {}): {}".format(
            e.cmd, e.returncode, " ".join(str(e.output).split())
        )
        raise RuntimeError(msg) from e
    if "All good!" not in output:
        msg = "Create database has not ended successfully"
        raise RuntimeError(msg)


def copy_db_sync_executables(config: DbSyncConfig, build_method: str = "nix") -> None:
    """Copy the Cardano DB Sync executables built with the specified build method.

    Args:
        config: A DbSyncConfig instance with paths.
        build_method: Build method to use, either "nix" or "cabal" (defaults to "nix").
    """
    db_sync_dir = config.workdir / "cardano-db-sync"

    if build_method == "nix":
        db_sync_binary_location = db_sync_dir / "db-sync-node" / "bin" / "cardano-db-sync"
        db_tool_binary_location = db_sync_dir / "db-sync-tool" / "bin" / "cardano-db-tool"
        shutil.copy2(db_sync_binary_location, db_sync_dir / "_cardano-db-sync")
        shutil.copy2(db_tool_binary_location, db_sync_dir / "_cardano-db-tool")
        return

    try:
        find_db_cmd = [
            "find",
            str(db_sync_dir),
            "-name",
            "cardano-db-sync",
            "-executable",
            "-type",
            "f",
        ]
        output_find_db_cmd = (
            subprocess.check_output(find_db_cmd, stderr=subprocess.STDOUT, timeout=15)
            .decode("utf-8")
            .strip()
        )
        LOGGER.info(f"Find cardano-db-sync output: {output_find_db_cmd}")
        shutil.copy2(output_find_db_cmd, db_sync_dir / "_cardano-db-sync")

        find_db_tool_cmd = [
            "find",
            str(db_sync_dir),
            "-name",
            "cardano-db-tool",
            "-executable",
            "-type",
            "f",
        ]
        output_find_db_tool_cmd = (
            subprocess.check_output(find_db_tool_cmd, stderr=subprocess.STDOUT, timeout=15)
            .decode("utf-8")
            .strip()
        )

        LOGGER.info(f"Find cardano-db-tool output: {output_find_db_tool_cmd}")
        shutil.copy2(output_find_db_tool_cmd, db_sync_dir / "_cardano-db-tool")

    except subprocess.CalledProcessError as e:
        msg = "command '{}' return with error (code {}): {}".format(
            e.cmd, e.returncode, " ".join(str(e.output).split())
        )
        raise RuntimeError(msg) from e


def get_db_sync_version(config: DbSyncConfig) -> tuple[str, str]:
    """Retrieve the version of the Cardano DB Sync executable.

    Args:
        config: A DbSyncConfig instance with paths.

    Returns:
        tuple[str, str]: A tuple containing the version string and git revision.
    """
    db_sync_dir = config.workdir / "cardano-db-sync"
    db_sync_binary = db_sync_dir / "_cardano-db-sync"
    try:
        cmd = [str(db_sync_binary), "--version"]
        output = (
            subprocess.check_output(cmd, cwd=str(db_sync_dir), stderr=subprocess.STDOUT)
            .decode("utf-8")
            .strip()
        )
        cardano_db_sync_version = output.split("git revision ")[0].strip()
        cardano_db_sync_git_revision = output.split("git revision ")[1].strip()
        return str(cardano_db_sync_version), str(cardano_db_sync_git_revision)
    except subprocess.CalledProcessError as e:
        msg = "command '{}' return with error (code {}): {}".format(
            e.cmd, e.returncode, " ".join(str(e.output).split())
        )
        raise RuntimeError(msg) from e


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
    config: DbSyncConfig, snapshot_file: str | Path, remove_ledger_dir: str = "yes"
) -> int:
    """Restore the Cardano DB Sync database from a snapshot.

    Args:
        config: A DbSyncConfig instance with paths and settings.
        snapshot_file: Path to the snapshot file to restore.
        remove_ledger_dir: Whether to remove the existing ledger directory (defaults to "yes").

    Returns:
        int: Restoration time in seconds.
    """
    db_sync_dir = config.workdir / "cardano-db-sync"
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

    # set tmp to local dir in current partition due to buildkite agent space
    # limitation on /tmp which is not big enough for snapshot restoration
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


def create_db_sync_snapshot_stage_1(config: DbSyncConfig) -> str:
    """Perform the first stage of creating a DB Sync snapshot.

    Args:
        config: A DbSyncConfig instance with paths and settings.

    Returns:
        str: The command to run for stage 2 of snapshot creation.
    """
    db_sync_dir = config.workdir / "cardano-db-sync"
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


def create_db_sync_snapshot_stage_2(config: DbSyncConfig, stage_2_cmd: str) -> str:
    """Perform the second stage of creating a DB Sync snapshot.

    Args:
        config: A DbSyncConfig instance with paths and settings.
        stage_2_cmd: The command to run for stage 2 (generated by stage 1).

    Returns:
        str: Path to the created snapshot file.
    """
    db_sync_dir = config.workdir / "cardano-db-sync"

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


def get_db_sync_tip(config: DbSyncConfig) -> DbSyncTip | None:
    """Retrieve the tip information from the Cardano DB Sync database.

    Args:
        config: A DbSyncConfig instance with database connection settings.

    Returns:
        DbSyncTip: Tip information with epoch, block, and slot numbers.
        None: If tip data cannot be retrieved after retries.
    """
    p = subprocess.Popen(
        [
            "psql",
            "-P",
            "pager=off",
            "-qt",
            "-U",
            config.pg_user,
            "-d",
            config.pg_dbname,
            "-c",
            "select epoch_no, block_no, slot_no from block order by id desc limit 1;",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    should_try = True
    counter = 0

    while should_try:
        output_string = ""
        try:
            outs, errs = p.communicate(timeout=180)
            output_string = outs.decode("utf-8")
            epoch_no_str, block_no_str, slot_no_str = [
                e.strip() for e in outs.decode("utf-8").split("|")
            ]
            return DbSyncTip(
                epoch_no=int(epoch_no_str),
                block_no=int(block_no_str),
                slot_no=int(slot_no_str),
            )
        except Exception:
            if counter > 5:
                should_try = False
                emergency_upload_artifacts(config, [])
                LOGGER.exception("Failed to get the tip")
                p.kill()
                raise
            err_msg = errs.decode("utf-8") if errs else "N/A"
            LOGGER.exception(
                f"db-sync tip data unavailable, possible postgress failure. "
                f"Output from psql: {output_string}, errs: {err_msg}"
            )
            counter += 1
            time.sleep(ONE_MINUTE)

    return None


def get_db_sync_progress(config: DbSyncConfig) -> float | None:
    """Calculate the synchronization progress of the Cardano DB Sync database.

    Args:
        config: A DbSyncConfig instance with database connection settings.

    Returns:
        float: Sync progress percentage, or None if unavailable.
    """
    p = subprocess.Popen(
        [
            "psql",
            "-P",
            "pager=off",
            "-qt",
            "-U",
            config.pg_user,
            "-d",
            config.pg_dbname,
            "-c",
            "select 100 * (extract (epoch from (max (time) at time zone 'UTC')) "
            "- extract (epoch from (min (time) at time zone 'UTC'))) "
            "/ (extract (epoch from (now () at time zone 'UTC')) "
            "- extract (epoch from (min (time) at time zone 'UTC'))) as sync_percent from block ;",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    should_try = True
    counter = 0

    while should_try:
        progress_string = ""
        try:
            outs, _errs = p.communicate(timeout=300)
            progress_string = outs.decode("utf-8")
            db_sync_progress = round(float(progress_string), 2)
        except Exception:
            if counter > 5:
                should_try = False
                emergency_upload_artifacts(config, [])
                p.kill()
                raise
            LOGGER.exception(
                "db-sync progress unavailable, possible postgress failure. "
                f"Output from psql: {progress_string}"
            )
            counter += 1
            time.sleep(ONE_MINUTE)
        else:
            return db_sync_progress
    return None


def _check_for_rollback(
    config: DbSyncConfig,
    current_progress: float,
    db_sync_progress: float,
    last_rollback_time: float,
    rollback_counter: int,
    counter: int,
    perf_stats: list[dict],
) -> tuple[int, float]:
    """Check for rollback and handle rollback counter.

    Args:
        config: A DbSyncConfig instance with paths.
        current_progress: Current sync progress percentage.
        db_sync_progress: Previous sync progress percentage.
        last_rollback_time: Timestamp of last rollback detection.
        rollback_counter: Current rollback counter value.
        counter: Current loop iteration counter.
        perf_stats: Performance statistics list.

    Returns:
        tuple[int, float]: Updated rollback_counter and last_rollback_time.

    Raises:
        Exception: If rollback counter exceeds threshold.
    """
    if current_progress < db_sync_progress and db_sync_progress > 3:
        LOGGER.info(
            "Progress decreasing - current progress: "
            f"{current_progress} VS previous: {db_sync_progress}."
        )
        LOGGER.info("Possible rollback... Printing last 10 lines of log")
        helpers.print_last_n_lines(config.db_sync_log_file, 10)
        if time.perf_counter() - last_rollback_time > 10 * ONE_MINUTE:
            LOGGER.info(
                "Resetting previous rollback counter as there was no progress decrease "
                "for more than 10 minutes"
            )
            rollback_counter = 0
        last_rollback_time = time.perf_counter()
        rollback_counter += 1
        LOGGER.info(f"Rollback counter: {rollback_counter} out of 15")
    if rollback_counter > 15:
        LOGGER.info(f"Progress decreasing for {rollback_counter * counter} minutes.")
        LOGGER.exception("Shutting down all services and emergency uploading artifacts")
        emergency_upload_artifacts(config, perf_stats)
        msg = "Rollback taking too long. Shutting down..."
        raise Exception(msg)
    return rollback_counter, last_rollback_time


def _log_sync_progress(config: DbSyncConfig, env: str, start_sync: float) -> float:
    """Log node and db sync progress information.

    Args:
        config: A DbSyncConfig instance with paths.
        env: Environment name.
        start_sync: Sync start timestamp.

    Returns:
        float: Current db sync progress percentage.
    """
    tip = node.get_current_tip(env)
    LOGGER.info(
        f"node progress [%]: {tip.sync_progress}, epoch: {tip.epoch}, "
        f"block: {tip.block}, slot: {tip.slot}, era: {tip.era}"
    )
    db_sync_tip = get_db_sync_tip(config)
    assert db_sync_tip is not None  # TODO: refactor
    db_sync_progress = get_db_sync_progress(config)
    assert db_sync_progress is not None  # TODO: refactor
    sync_time_h_m_s = str(timedelta(seconds=(time.perf_counter() - start_sync)))
    LOGGER.info(
        f"db sync progress [%]: {db_sync_progress}, sync time [h:m:s]: {sync_time_h_m_s}, "
        f"epoch: {db_sync_tip.epoch_no}, block: {db_sync_tip.block_no}, slot: {db_sync_tip.slot_no}"
    )
    helpers.print_last_n_lines(config.db_sync_log_file, 5)
    return db_sync_progress


def _collect_perf_stats(
    config: DbSyncConfig,
    db_sync_process: psutil.Process,
    start_sync: float,
    perf_stats: list[dict],
) -> None:
    """Collect performance statistics and write to file.

    Args:
        config: A DbSyncConfig instance with paths.
        db_sync_process: DB sync process object.
        start_sync: Sync start timestamp.
        perf_stats: Performance statistics list to append to.
    """
    time_point = int(time.perf_counter() - start_sync)
    db_sync_tip = get_db_sync_tip(config)
    assert db_sync_tip is not None  # TODO: refactor
    cpu_usage = db_sync_process.cpu_percent(interval=None)
    rss_mem_usage = db_sync_process.memory_info()[0]
    stats_data_point = PerfStats(
        time=time_point,
        slot_no=db_sync_tip.slot_no,
        cpu_percent_usage=cpu_usage,
        rss_mem_usage=rss_mem_usage,
    )
    perf_stats.append(dataclasses.asdict(stats_data_point))
    helpers.write_json_to_file(config.perf_stats_file, perf_stats)


def wait_for_db_to_sync(
    config: DbSyncConfig, sync_percentage: float = 99.9, perf_stats: list[dict] | None = None
) -> tuple[int, list[dict]]:
    """Wait for the Cardano DB Sync database to fully synchronize.

    Args:
        config: A DbSyncConfig instance with database connection settings and paths.
        sync_percentage: Target sync percentage (defaults to 99.9).
        perf_stats: Optional list to accumulate performance statistics (creates new list if None).

    Returns:
        tuple[int, list[dict]]: A tuple containing sync time in seconds and
            performance statistics list.
    """
    if perf_stats is None:
        perf_stats = []
    perf_stats.clear()

    start_sync = time.perf_counter()
    last_rollback_time = time.perf_counter()
    db_sync_progress = get_db_sync_progress(config)
    assert db_sync_progress is not None  # TODO: refactor
    buildkite_timeout_in_sec = 1828000
    counter = 0
    rollback_counter = 0

    db_sync_process = helpers.manage_process(proc_name="cardano-db-sync", action="get")
    log_frequency = get_log_output_frequency(config.env)

    LOGGER.info("--- Db sync monitoring")
    while db_sync_progress < sync_percentage:
        sync_time_in_sec = time.perf_counter() - start_sync
        if sync_time_in_sec + 5 * ONE_MINUTE > buildkite_timeout_in_sec:
            emergency_upload_artifacts(config, perf_stats)
            msg = "Emergency uploading artifacts before buid timeout exception..."
            raise Exception(msg)
        if counter % 5 == 0:
            current_progress = get_db_sync_progress(config)
            assert current_progress is not None  # TODO: refactor
            rollback_counter, last_rollback_time = _check_for_rollback(
                config=config,
                current_progress=current_progress,
                db_sync_progress=db_sync_progress,
                last_rollback_time=last_rollback_time,
                rollback_counter=rollback_counter,
                counter=counter,
                perf_stats=perf_stats,
            )
        if counter % log_frequency == 0:
            db_sync_progress = _log_sync_progress(
                config=config,
                env=config.env,
                start_sync=start_sync,
            )

        try:
            _collect_perf_stats(
                config=config,
                db_sync_process=db_sync_process,
                start_sync=start_sync,
                perf_stats=perf_stats,
            )
        except Exception:
            end_sync = time.perf_counter()
            db_full_sync_time_in_secs = int(end_sync - start_sync)
            LOGGER.exception("Unexpected error during sync process")
            emergency_upload_artifacts(config, perf_stats)
            return db_full_sync_time_in_secs, perf_stats
        time.sleep(ONE_MINUTE)
        counter += 1

    end_sync = time.perf_counter()
    sync_time_seconds = int(end_sync - start_sync)
    LOGGER.info(f"db sync progress [%] before finalizing process: {db_sync_progress}")
    return sync_time_seconds, perf_stats


def get_total_db_size(config: DbSyncConfig) -> str:
    """Fetch the total size of the Cardano DB Sync database.

    Args:
        config: A DbSyncConfig instance with database connection settings.

    Returns:
        str: Human-readable database size.
    """
    cmd = [
        "psql",
        "-P",
        "pager=off",
        "-qt",
        "-U",
        config.pg_user,
        "-d",
        config.pg_dbname,
        "-c",
        f"SELECT pg_size_pretty( pg_database_size('{config.pg_dbname}') );",
    ]
    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
        outs, errs = p.communicate(timeout=60)
        if errs:
            LOGGER.error(f"Error in get database size: {errs}")
        return outs.rstrip().strip()
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        p.kill()
        raise
    except Exception:
        p.kill()
        raise


def start_db_sync(config: DbSyncConfig, start_args: str = "", first_start: str = "True") -> None:
    """Start the Cardano DB Sync process.

    Args:
        config: A DbSyncConfig instance with paths and settings.
        start_args: Additional start arguments for db-sync (optional).
        first_start: Whether this is the first start (defaults to "True").
    """
    helpers.export_env_var("DB_SYNC_START_ARGS", start_args)
    helpers.export_env_var("FIRST_START", f"{first_start}")
    helpers.export_env_var("ENVIRONMENT", config.env)
    helpers.export_env_var("LOG_FILEPATH", str(config.db_sync_log_file))

    script_path = config.workdir / "sync_tests" / "scripts" / "db-sync-start.sh"
    try:
        cmd = [str(script_path)]
        subprocess.Popen(
            cmd, cwd=str(config.workdir), stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
    except subprocess.CalledProcessError as e:
        msg = "command '{}' return with error (code {}): {}".format(
            e.cmd, e.returncode, " ".join(str(e.output).split())
        )
        raise RuntimeError(msg) from e

    not_found = True
    counter = 0

    while not_found:
        if counter > 10 * ONE_MINUTE:
            LOGGER.error(f"ERROR: waited {counter} seconds and the db-sync was not started")
            sys.exit(1)

        for proc in psutil.process_iter():
            if "cardano-db-sync" in proc.name():
                LOGGER.info(f"db-sync process present: {proc}")
                not_found = False
                return
        LOGGER.info("Waiting for db-sync to start")
        counter += ONE_MINUTE
        time.sleep(ONE_MINUTE)


def get_file_size(file: str) -> int:
    """Return the size of a specified file in megabytes."""
    file_stats = os.stat(file)
    file_size_in_mb = int(file_stats.st_size / (1000 * 1000))
    return file_size_in_mb


def setup_postgres(config: DbSyncConfig, pg_port: str | None = None) -> None:
    """Set up PostgreSQL for use with Cardano DB Sync.

    Args:
        config: A DbSyncConfig instance with PostgreSQL settings.
        pg_port: Optional PostgreSQL port override (defaults to config.pg_port).
    """
    postgres_port = pg_port if pg_port is not None else config.pg_port
    helpers.export_env_var("POSTGRES_DIR", str(config.pg_dir))
    helpers.export_env_var("PGHOST", config.pg_host)
    helpers.export_env_var("PGUSER", config.pg_user)
    helpers.export_env_var("PGPORT", postgres_port)

    script_path = config.workdir / "sync_tests" / "scripts" / "postgres-start.sh"
    try:
        cmd = [str(script_path), str(config.pg_dir), "-k"]
        output = (
            subprocess.check_output(cmd, cwd=str(config.workdir), stderr=subprocess.STDOUT)
            .decode("utf-8")
            .strip()
        )
        LOGGER.info(f"Setup postgres script output: {output}")
    except subprocess.CalledProcessError as e:
        msg = "command '{}' return with error (code {}): {}".format(
            e.cmd, e.returncode, " ".join(str(e.output).split())
        )
        raise RuntimeError(msg) from e


def list_databases(config: DbSyncConfig) -> None:
    """List all databases available in the PostgreSQL instance.

    Args:
        config: A DbSyncConfig instance with PostgreSQL settings.
    """
    cmd = ["psql", "-U", config.pg_user, "-l"]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")

    try:
        outs, errs = p.communicate(timeout=60)
        LOGGER.info(f"List databases: {outs}")
        if errs:
            LOGGER.error(f"Error in list databases: {errs}")
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        p.kill()
        raise


def get_db_schema(config: DbSyncConfig) -> dict:
    """Retrieve the schema of the Cardano DB Sync database.

    Args:
        config: A DbSyncConfig instance with database connection settings.

    Returns:
        dict: A dictionary mapping table names to their schema definitions.
    """
    conn = None
    try:
        conn = psycopg2.connect(database=config.pg_dbname, user=config.pg_user)
        cursor = conn.cursor()
        get_all_tables = (
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        )
        cursor.execute(get_all_tables)
        tabels = cursor.fetchall()

        db_schema = {}
        for table in tabels:
            table_name = table[0]
            get_table_fields_and_attributes = (
                'SELECT a.attname as "Column", pg_catalog.format_type(a.atttypid, a.atttypmod)'
                ' as "Datatype" FROM pg_catalog.pg_attribute a WHERE a.attnum > 0'
                " AND NOT a.attisdropped AND a.attrelid = ( SELECT c.oid"
                " FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n"
                f" ON n.oid = c.relnamespace WHERE c.relname ~ '^{table_name}$'"
                " AND pg_catalog.pg_table_is_visible(c.oid));"
            )
            cursor.execute(get_table_fields_and_attributes)
            table_with_attributes = cursor.fetchall()
            attributes = list(table_with_attributes)
            db_schema[str(table_name)] = attributes
        cursor.close()
        conn.commit()
        conn.close()
    except (Exception, psycopg2.DatabaseError):
        LOGGER.exception("Error")
    finally:
        if conn is not None:
            conn.close()

    return db_schema


def get_db_indexes(config: DbSyncConfig) -> dict:
    """Fetch the indexes of tables in the Cardano DB Sync database.

    Args:
        config: A DbSyncConfig instance with database connection settings.

    Returns:
        dict: A dictionary mapping table names to their index names.
    """
    conn = None
    try:
        conn = psycopg2.connect(database=config.pg_dbname, user=config.pg_user)
        cursor = conn.cursor()

        get_all_tables = (
            "select tbl.relname as table_name from pg_index pgi"
            " join pg_class idx on idx.oid = pgi.indexrelid join pg_namespace insp"
            " on insp.oid = idx.relnamespace join pg_class tbl"
            " on tbl.oid = pgi.indrelid join pg_namespace tnsp"
            " on tnsp.oid = tbl.relnamespace where pgi.indisunique and tnsp.nspname = 'public';"
        )
        cursor.execute(get_all_tables)
        tables = cursor.fetchall()
        all_indexes = {}

        for table in tables:
            table_name = table[0]
            get_table_and_index = (
                "select tbl.relname as table_name, idx.relname as index_name"
                " from pg_index pgi join pg_class idx on idx.oid = pgi.indexrelid"
                " join pg_namespace insp on insp.oid = idx.relnamespace join pg_class tbl"
                " on tbl.oid = pgi.indrelid join pg_namespace tnsp"
                " on tnsp.oid = tbl.relnamespace where pgi.indisunique"
                f" and tnsp.nspname = 'public' and tbl.relname = '{table_name}';"
            )
            cursor.execute(get_table_and_index)
            table_and_index = cursor.fetchall()
            indexes = []
            for _table, index in table_and_index:
                indexes.append(index)
            all_indexes[str(table_name)] = indexes
        cursor.close()
        conn.commit()
        conn.close()
    except (Exception, psycopg2.DatabaseError):
        LOGGER.exception("Error")
        return {}
    else:
        return all_indexes
    finally:
        if conn is not None:
            conn.close()


def check_database(fn: tp.Callable, err_msg: str, expected_value: tp.Any) -> Exception | None:
    """Validate the database using a specified function and expected value."""
    try:
        assert_that(fn()).described_as(err_msg).is_equal_to(expected_value)
    except AssertionError as e:
        helpers.print_message(f"Warning - validation errors: {e}\n\n", type="warn")
        return e
    return None


def create_sync_stats_chart(config: DbSyncConfig) -> None:
    """Create a chart showing sync statistics.

    Args:
        config: A DbSyncConfig instance with paths.
    """
    db_sync_dir = config.workdir / "cardano-db-sync"
    fig = plt.figure(figsize=(14, 10))

    ax_epochs = fig.add_axes((0.05, 0.05, 0.9, 0.35))
    ax_epochs.set(xlabel="epochs [number]", ylabel="time [min]")
    ax_epochs.set_title("Epochs Sync Times")

    with open(config.epoch_sync_times_file) as json_db_dump_file:
        epoch_sync_times = json.load(json_db_dump_file)

    epochs = [e["no"] for e in epoch_sync_times]
    epoch_times = [e["seconds"] / 60 for e in epoch_sync_times]
    ax_epochs.bar(epochs, epoch_times)

    ax_perf = fig.add_axes((0.05, 0.5, 0.9, 0.45))
    ax_perf.set(xlabel="time [min]", ylabel="RSS [B]")
    ax_perf.set_title("RSS usage")

    with open(config.perf_stats_file) as json_db_dump_file:
        perf_stats = json.load(json_db_dump_file)

    times = [e["time"] / 60 for e in perf_stats]
    rss_mem_usage = [e["rss_mem_usage"] for e in perf_stats]

    ax_perf.plot(times, rss_mem_usage)
    chart_path = db_sync_dir / config.chart_name
    fig.savefig(chart_path)
