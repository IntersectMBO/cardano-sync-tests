"""DB Sync orchestration utilities and re-export layer."""

from __future__ import annotations

import dataclasses
import logging
import os
import pathlib as pl
import platform
import shutil
import subprocess
import sys
import time
import typing as tp
from datetime import timedelta

import psutil

from sync_tests.utils import artifacts
from sync_tests.utils import helpers
from sync_tests.utils import node
from sync_tests.utils.db_sync import config as db_sync_config
from sync_tests.utils.db_sync import data as db_sync_data
from sync_tests.utils.db_sync import postgres
from sync_tests.utils.db_sync import snapshots
from sync_tests.utils.db_sync.config import DbSyncConfig
from sync_tests.utils.db_sync.config import DbSyncTip
from sync_tests.utils.db_sync.config import PerfStats
from sync_tests.utils.db_sync.perf_utils import enrich_perf_stats_with_era
from sync_tests.utils.path_utils import get_db_sync_dir
from sync_tests.utils.path_utils import get_repo_root

LOGGER = logging.getLogger(__name__)

ONE_MINUTE = 60


def create_db_sync_config(
    env: str,
    workdir: pl.Path | None = None,
    pg_host: str = "localhost",
    pg_port: str = "5432",
    pg_user: str | None = None,
    pg_dbname: str | None = None,
    pg_dir: pl.Path | None = None,
) -> DbSyncConfig:
    """Create a DbSyncConfig instance from environment variables and parameters."""
    return db_sync_config.create_db_sync_config(
        env=env,
        workdir=workdir,
        pg_host=pg_host,
        pg_port=pg_port,
        pg_user=pg_user,
        pg_dbname=pg_dbname,
        pg_dir=pg_dir,
    )


def get_machine_name() -> str:
    """Retrieve the name of the machine."""
    return platform.node()


def get_last_perf_stats_point(perf_stats: list[dict]) -> PerfStats:
    """Retrieve the last performance statistics data point, or initializes one if none exists.

    Args:
        perf_stats: A list of performance statistics dictionaries.

    Returns:
        PerfStats: The last performance statistics point, or a default one if none exists.
    """
    try:
        last_dict = perf_stats[-1]
        # Filter to only the fields PerfStats accepts; enriched dicts carry extra era keys.
        _perf_fields = {f.name for f in dataclasses.fields(PerfStats)}
        filtered = {k: v for k, v in last_dict.items() if k in _perf_fields}
        return PerfStats(**filtered)
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


def get_era_activation_data(config: DbSyncConfig) -> list[dict]:
    """Retrieve era activation metadata from the db-sync database."""
    return postgres.get_era_activation_data(config)


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


def copy_db_sync_executables(_config: DbSyncConfig, build_method: str = "nix") -> None:
    """Copy the Cardano DB Sync executables built with the specified build method.

    Args:
        config: A DbSyncConfig instance with paths.
        build_method: Build method to use, either "nix" or "cabal" (defaults to "nix").
    """
    db_sync_dir = get_db_sync_dir()

    if build_method == "nix":
        db_sync_binary_location = db_sync_dir / "db-sync-node" / "bin" / "cardano-db-sync"
        db_tool_binary_location = db_sync_dir / "db-sync-tool" / "bin" / "cardano-db-tool"

        # These copies are convenience wrappers used by some older scripts.
        # If permissions prevent writing into the repo directory (e.g., root-owned
        # files from a previous run), we log and continue instead of failing the run.
        try:
            shutil.copy2(db_sync_binary_location, db_sync_dir / "_cardano-db-sync")
            shutil.copy2(db_tool_binary_location, db_sync_dir / "_cardano-db-tool")
            LOGGER.info("Copied db-sync and db-tool executables to _cardano-* wrappers")
        except PermissionError as e:
            LOGGER.warning(
                "PermissionError while copying db-sync executables to _cardano-* wrappers: "
                "%s. Continuing, since db-sync binaries in db-sync-node/db-sync-tool "
                "are sufficient for running tests.",
                e,
            )
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
        LOGGER.info("Find cardano-db-sync output: %s", output_find_db_cmd)
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

        LOGGER.info("Find cardano-db-tool output: %s", output_find_db_tool_cmd)
        shutil.copy2(output_find_db_tool_cmd, db_sync_dir / "_cardano-db-tool")

    except subprocess.CalledProcessError as e:
        msg = "command '{}' return with error (code {}): {}".format(
            e.cmd, e.returncode, " ".join(str(e.output).split())
        )
        raise RuntimeError(msg) from e


def get_db_sync_version(_config: DbSyncConfig) -> tuple[str, str]:
    """Retrieve the version of the Cardano DB Sync executable.

    Args:
        _config: A DbSyncConfig instance (reserved for future use).

    Returns:
        A ``(version_string, git_revision)`` tuple parsed from the binary output.
    """
    db_sync_dir = get_db_sync_dir()
    # Use the nix-built binary directly; avoids depending on the _cardano-db-sync
    # convenience copy which may not exist when copy_db_sync_executables was skipped
    # (e.g. snapshot restoration tests) or failed due to a PermissionError.
    db_sync_binary = db_sync_dir / "db-sync-node" / "bin" / "cardano-db-sync"
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
        RuntimeError: If rollback counter exceeds threshold.
    """
    if current_progress < db_sync_progress and db_sync_progress > 3:
        LOGGER.info(
            "Progress decreasing - current progress: %s VS previous: %s.",
            current_progress,
            db_sync_progress,
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
        LOGGER.info("Rollback counter: %s out of 15", rollback_counter)
    if rollback_counter > 15:
        LOGGER.info("Progress decreasing for %s minutes.", rollback_counter * counter)
        LOGGER.error("Shutting down all services and emergency uploading artifacts")
        era_activation = postgres.get_era_activation_data(config)
        artifacts.emergency_upload_artifacts(config, perf_stats, era_activation)
        msg = "Rollback taking too long. Shutting down..."
        raise RuntimeError(msg)
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
    try:
        tip = node.get_current_tip(env)
    except Exception:
        LOGGER.warning("Node tip unavailable while logging db-sync progress", exc_info=True)
    else:
        LOGGER.info(
            "node progress [%%]: %s, epoch: %s, block: %s, slot: %s, era: %s",
            tip.sync_progress,
            tip.epoch,
            tip.block,
            tip.slot,
            tip.era,
        )
    try:
        db_sync_tip = postgres.get_db_sync_tip(config)
    except Exception:
        LOGGER.warning("db-sync tip unavailable during progress log", exc_info=True)
        return 0.0
    # Handle case where db-sync hasn't started syncing yet
    if db_sync_tip is None:
        LOGGER.info("db-sync tip not available yet - db-sync may not have started syncing")
        return 0.0
    db_sync_progress = postgres.get_db_sync_progress(config)
    # Handle case where progress is None (db-sync hasn't started yet)
    if db_sync_progress is None:
        LOGGER.info("db-sync progress not available yet - db-sync may not have started syncing")
        return 0.0
    sync_time_h_m_s = str(timedelta(seconds=(time.perf_counter() - start_sync)))
    LOGGER.info(
        "db sync progress [%%]: %s, sync time [h:m:s]: %s, epoch: %s, block: %s, slot: %s",
        db_sync_progress,
        sync_time_h_m_s,
        db_sync_tip.epoch_no,
        db_sync_tip.block_no,
        db_sync_tip.slot_no,
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
    # Handle case where db-sync hasn't started syncing yet
    if db_sync_tip is None:
        LOGGER.debug("db-sync tip not available yet - skipping perf stats collection")
        return
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
    # Handle case where db-sync hasn't started syncing yet
    if db_sync_progress is None:
        LOGGER.info("db-sync hasn't started syncing yet, waiting for initial progress...")
        db_sync_progress = 0.0
    buildkite_timeout_in_sec = 1828000
    counter = 0
    rollback_counter = 0

    db_sync_process = helpers.manage_process(proc_name="cardano-db-sync", action="get")
    if db_sync_process is None:
        msg = "cardano-db-sync process not found while collecting performance stats"
        raise RuntimeError(msg)
    log_frequency = get_log_output_frequency(config.env)

    LOGGER.info("--- Db sync monitoring")
    _emergency_uploaded = False
    while db_sync_progress < sync_percentage:
        sync_time_in_sec = time.perf_counter() - start_sync
        if sync_time_in_sec + 5 * ONE_MINUTE > buildkite_timeout_in_sec:
            era_activation = postgres.get_era_activation_data(config)
            artifacts.emergency_upload_artifacts(config, perf_stats, era_activation)
            _emergency_uploaded = True
            msg = "Emergency uploading artifacts before build timeout exception..."
            raise RuntimeError(msg)
        if counter % 5 == 0:
            current_progress = postgres.get_db_sync_progress(config)
            # Handle case where progress is None (db-sync hasn't started yet)
            if current_progress is None:
                current_progress = 0.0
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
            if not _emergency_uploaded:
                era_activation = postgres.get_era_activation_data(config)
                artifacts.emergency_upload_artifacts(config, perf_stats, era_activation)
                _emergency_uploaded = True
            return db_full_sync_time_in_secs, perf_stats
        time.sleep(ONE_MINUTE)
        counter += 1

    end_sync = time.perf_counter()
    sync_time_seconds = int(end_sync - start_sync)
    LOGGER.info("db sync progress [%%] before finalizing process: %s", db_sync_progress)
    era_activation = postgres.get_era_activation_data(config)
    enriched_perf_stats = enrich_perf_stats_with_era(perf_stats, era_activation)
    helpers.write_json_to_file(config.perf_stats_file, enriched_perf_stats)
    return sync_time_seconds, enriched_perf_stats


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
    # Ensure CARDANO_NODE_SOCKET_PATH is set so db-sync startup script can find the socket
    # The socket is at repo_root/db/node.socket, not in cardano-db-sync directory
    node_socket_path = os.environ.get("CARDANO_NODE_SOCKET_PATH")
    if node_socket_path:
        helpers.export_env_var("CARDANO_NODE_SOCKET_PATH", node_socket_path)
    else:
        # Fallback: construct socket path from repo root
        repo_root = get_repo_root()
        socket_path = repo_root / "db" / "node.socket"
        helpers.export_env_var("CARDANO_NODE_SOCKET_PATH", str(socket_path))

    repo_root = get_repo_root()
    script_path = repo_root / "sync_tests" / "scripts" / "db-sync-start.sh"
    # The script expects to run from cardano-db-sync where db-sync-node/bin/cardano-db-sync exists.
    db_sync_dir = get_db_sync_dir()
    LOGGER.info("Starting db-sync with script: %s", script_path)
    LOGGER.info("Working directory (for script): %s", db_sync_dir)
    LOGGER.info(
        "Environment variables: ENVIRONMENT=%s, LOG_FILEPATH=%s",
        config.env,
        config.db_sync_log_file,
    )

    # Check if script exists and is executable
    if not script_path.exists():
        msg = f"db-sync startup script not found: {script_path}"
        raise RuntimeError(msg)
    if not os.access(script_path, os.X_OK):
        LOGGER.warning("db-sync startup script is not executable, attempting to make it executable")
        os.chmod(script_path, 0o755)

    try:
        cmd = [str(script_path)]
        # Launch the script - it uses nix develop which might take time
        # We don't wait for it, but we'll check if it exits immediately
        # Run from cardano-db-sync directory where the binaries are located
        proc = subprocess.Popen(
            cmd,
            cwd=str(db_sync_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        LOGGER.info("Launched db-sync startup script (PID: %s)", proc.pid)

        # The script runs db-sync in the background and exits, which is normal.
        # Wait a bit for the script to finish and db-sync to start, then check for errors.
        time.sleep(3)
        stdout, _ = proc.communicate()

        # Check if db-sync process started successfully
        db_sync_found = False
        for proc_item in psutil.process_iter():
            try:
                if "cardano-db-sync" in proc_item.name():
                    db_sync_found = True
                    LOGGER.info("db-sync process found: %s (PID: %s)", proc_item, proc_item.pid)
                    break
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        # If script output contains errors and db-sync didn't start, that's a problem
        if (
            stdout
            and not db_sync_found
            and ("Error" in stdout or "FATAL" in stdout or "Cannot find" in stdout)
        ):
            LOGGER.error("db-sync startup script output contains errors:\n%s", stdout)
            msg = f"db-sync startup script failed. Output:\n{stdout}"
            raise RuntimeError(msg)

        # If we found db-sync process, we're good (even if script exited)
        if db_sync_found:
            LOGGER.info("db-sync process started successfully")
            return

    except Exception:
        LOGGER.exception("Failed to start db-sync script")
        raise

    not_found = True
    counter = 0

    while not_found:
        if counter > 10 * ONE_MINUTE:
            LOGGER.error("ERROR: waited %s seconds and the db-sync was not started", counter)
            # Check if script process is still running
            if proc.poll() is not None:
                stdout, _ = proc.communicate()
                if stdout:
                    LOGGER.error("db-sync startup script output:\n%s", stdout)
                LOGGER.error("db-sync startup script exited with code: %s", proc.returncode)
            else:
                LOGGER.warning(
                    "db-sync startup script process (PID: %s) is still running", proc.pid
                )
            # Check logfile for any errors
            if config.db_sync_log_file.exists() and config.db_sync_log_file.stat().st_size > 0:
                LOGGER.error(
                    "db-sync logfile contents (%s bytes):",
                    config.db_sync_log_file.stat().st_size,
                )
                helpers.print_last_n_lines(config.db_sync_log_file, 50)
            else:
                LOGGER.error("db-sync logfile is empty or missing: %s", config.db_sync_log_file)
            # List all processes to help debug
            LOGGER.error("Checking for any db-sync related processes:")
            for proc_item in psutil.process_iter():
                try:
                    proc_name = proc_item.name()
                    if "db" in proc_name.lower() or "sync" in proc_name.lower():
                        LOGGER.error("  Found process: %s (PID: %s)", proc_name, proc_item.pid)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            sys.exit(1)

        for proc_item in psutil.process_iter():
            try:
                if "cardano-db-sync" in proc_item.name():
                    LOGGER.info(
                        "db-sync process found: %s (PID: %s)",
                        proc_item,
                        proc_item.pid,
                    )
                    not_found = False
                    break
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        if not not_found:
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


def export_epoch_sync_times_from_db(
    config: DbSyncConfig, file: str | pl.Path, snapshot_epoch_no: int | str = 0
) -> str | None:
    """Export epoch synchronization times from the database to a file."""
    return db_sync_data.export_epoch_sync_times_from_db(config, file, snapshot_epoch_no)


def setup_postgres(config: DbSyncConfig, pg_port: str | None = None) -> None:
    """Set up PostgreSQL for use with Cardano DB Sync."""
    postgres.setup_postgres(config, pg_port=pg_port)


def stop_postgres(config: DbSyncConfig) -> None:
    """Stop the PostgreSQL instance started for this test workspace."""
    postgres.stop_postgres(config)


def finalize_session_disk_cleanup(config: DbSyncConfig) -> None:
    """Remove PGDATA under ``config.pg_dir``; optionally remove ledger-state (CI)."""
    # Lazy import avoids circular import: db_sync.__init__ is still loading if
    # disk_cleanup is imported at module top (conftest imports db_sync early).
    from sync_tests.utils.db_sync.disk_cleanup import (  # noqa: PLC0415
        finalize_session_disk_cleanup as _disk_cleanup_impl,
    )

    _disk_cleanup_impl(config)


def stop_monitor(workdir: pl.Path) -> None:
    """Stop the background resource monitor started for this workspace."""
    postgres.stop_monitor(workdir)


def create_pgpass_file(config: DbSyncConfig) -> None:
    """Create a PostgreSQL password file for the specified environment."""
    postgres.create_pgpass_file(config)


def create_database() -> None:
    """Set up the PostgreSQL database for use with Cardano DB Sync."""
    postgres.create_database()


def list_databases(config: DbSyncConfig) -> None:
    """List all databases available in the PostgreSQL instance."""
    postgres.list_databases(config)


def get_db_sync_tip(config: DbSyncConfig) -> DbSyncTip | None:
    """Retrieve the tip information from the Cardano DB Sync database."""
    return postgres.get_db_sync_tip(config)


def get_db_sync_progress(config: DbSyncConfig) -> float | None:
    """Calculate the synchronization progress of the Cardano DB Sync database."""
    return postgres.get_db_sync_progress(config)


def get_total_db_size(config: DbSyncConfig) -> str:
    """Fetch the total size of the Cardano DB Sync database."""
    return postgres.get_total_db_size(config)


def upload_artifact(file: str, destination: str = "auto") -> None:
    """Upload an artifact using the configured artifact handler."""
    artifacts.upload_artifact(file, destination=destination)


def create_node_database_archive(config: DbSyncConfig) -> pl.Path:
    """Create an archive of the node database for the specified environment."""
    return artifacts.create_node_database_archive(config)


def set_buildkite_meta_data(key: str, value: tp.Any) -> None:
    """Set Buildkite metadata for the specified key and value."""
    artifacts.set_buildkite_meta_data(key, value)


def get_buildkite_meta_data(key: str) -> str:
    """Retrieve Buildkite metadata for the specified key."""
    return artifacts.get_buildkite_meta_data(key)


def get_latest_snapshot_url(env: str, args: tp.Any) -> str:
    """Retrieve the latest snapshot URL for the specified environment."""
    return snapshots.get_latest_snapshot_url(env, args)


def download_db_sync_snapshot(snapshot_url: str) -> str:
    """Download a db-sync snapshot from the specified URL."""
    return snapshots.download_db_sync_snapshot(snapshot_url)


def get_snapshot_sha_256_sum(snapshot_url: str) -> str | None:
    """Retrieve the expected sha256 checksum for the specified snapshot."""
    return snapshots.get_snapshot_sha_256_sum(snapshot_url)


def restore_db_sync_from_snapshot(
    config: DbSyncConfig, snapshot_file: str | pl.Path, remove_ledger_dir: str = "yes"
) -> float:
    """Restore db-sync from a snapshot file."""
    return snapshots.restore_db_sync_from_snapshot(config, snapshot_file, remove_ledger_dir)


def create_db_sync_snapshot_stage_1(config: DbSyncConfig) -> str:
    """Create a db-sync snapshot (stage 1) and return the snapshot file path."""
    return snapshots.create_db_sync_snapshot_stage_1(config)


def create_db_sync_snapshot_stage_2(config: DbSyncConfig, stage_2_cmd: str) -> str:
    """Create a db-sync snapshot (stage 2) and return the snapshot file path."""
    return snapshots.create_db_sync_snapshot_stage_2(config, stage_2_cmd)


def start_monitor(workdir: pl.Path, env: str) -> None:
    """Start the background resource monitor for this workspace.

    Args:
        workdir: The test working directory where monitor.pid and logs will be written.
        env: The environment name used to label the log file (e.g. ``mainnet``).
    """
    postgres.start_monitor(workdir, env)
