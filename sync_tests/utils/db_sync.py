import dataclasses
import logging
import os
import platform
import shutil
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

import psutil

from sync_tests.utils import artifacts
from sync_tests.utils import charts
from sync_tests.utils import helpers
from sync_tests.utils import node
from sync_tests.utils import postgres
from sync_tests.utils import snapshots

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


def set_node_socket_path_env_var_in_cwd(config: DbSyncConfig) -> None:
    """Set the node socket path environment variable in the current working directory.

    Args:
        config: A DbSyncConfig instance with paths.
    """
    node_dir = config.workdir / "cardano-node"
    socket_path = node_dir / "db" / "node.socket"
    helpers.export_env_var("CARDANO_NODE_SOCKET_PATH", str(socket_path))


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
        artifacts.emergency_upload_artifacts(config, perf_stats)
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
    db_sync_tip = postgres.get_db_sync_tip(config)
    assert db_sync_tip is not None  # TODO: refactor
    db_sync_progress = postgres.get_db_sync_progress(config)
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
    db_sync_tip = postgres.get_db_sync_tip(config)
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
    db_sync_progress = postgres.get_db_sync_progress(config)
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
            artifacts.emergency_upload_artifacts(config, perf_stats)
            msg = "Emergency uploading artifacts before buid timeout exception..."
            raise Exception(msg)
        if counter % 5 == 0:
            current_progress = postgres.get_db_sync_progress(config)
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
            artifacts.emergency_upload_artifacts(config, perf_stats)
            return db_full_sync_time_in_secs, perf_stats
        time.sleep(ONE_MINUTE)
        counter += 1

    end_sync = time.perf_counter()
    sync_time_seconds = int(end_sync - start_sync)
    LOGGER.info(f"db sync progress [%] before finalizing process: {db_sync_progress}")
    return sync_time_seconds, perf_stats


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


# Re-export functions from other modules for backward compatibility
from sync_tests.utils.artifacts import (
    create_node_database_archive,
    emergency_upload_artifacts,
    get_buildkite_meta_data,
    set_buildkite_meta_data,
    upload_artifact,
)

from sync_tests.utils.charts import create_sync_stats_chart

from sync_tests.utils.postgres import (
    check_database,
    create_database,
    create_pgpass_file,
    export_epoch_sync_times_from_db,
    get_db_indexes,
    get_db_schema,
    get_db_sync_progress,
    get_db_sync_tip,
    get_total_db_size,
    list_databases,
    setup_postgres,
)

from sync_tests.utils.snapshots import (
    create_db_sync_snapshot_stage_1,
    create_db_sync_snapshot_stage_2,
    download_and_extract_node_snapshot,
    download_db_sync_snapshot,
    get_latest_snapshot_url,
    get_snapshot_sha_256_sum,
    restore_db_sync_from_snapshot,
)
