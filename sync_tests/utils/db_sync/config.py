"""Configuration dataclasses and factory functions for DB Sync test runs."""

from __future__ import annotations

import dataclasses
import os
import pathlib as pl
import subprocess


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
    workdir: pl.Path

    # PostgreSQL configuration
    pg_host: str
    pg_port: str
    pg_user: str
    pg_dbname: str
    pg_dir: pl.Path

    # Paths
    perf_stats_file: pl.Path
    node_log_file: pl.Path
    db_sync_log_file: pl.Path
    epoch_sync_times_file: pl.Path
    postgres_log_file: pl.Path
    monitor_log_file: pl.Path
    monitor_stderr_log_file: pl.Path
    oom_log_file: pl.Path

    # Archive names
    node_archive_name: str
    db_sync_archive_name: str
    sync_data_archive_name: str
    perf_stats_archive_name: str
    monitor_archive_name: str

    # Chart
    chart_name: str

    # Optional fields
    node_pr: str | None = None
    node_branch: str | None = None
    node_version: str | None = None
    db_sync_branch: str | None = None
    db_sync_version: str | None = None


def create_db_sync_config(
    env: str,
    workdir: pl.Path | None = None,
    pg_host: str = "localhost",
    pg_port: str = "5432",
    pg_user: str | None = None,
    pg_dbname: str | None = None,
    pg_dir: pl.Path | None = None,
) -> DbSyncConfig:
    """Create a DbSyncConfig instance from environment variables and parameters.

    Args:
        env: Environment name (e.g., "preview", "preprod", "mainnet").
        workdir: Working directory path (defaults to current directory).
        pg_host: PostgreSQL host (defaults to "localhost").
        pg_port: PostgreSQL port (defaults to "5432").
        pg_user: PostgreSQL user (defaults to current system user).
        pg_dbname: PostgreSQL database name (defaults to env name).
        pg_dir: PostgreSQL data directory (defaults to ``workdir / "postgres"``).

    Returns:
        DbSyncConfig: A configuration instance with all paths and settings.
    """
    if workdir is None:
        workdir = pl.Path.cwd()
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
        pg_dir = workdir / "postgres"

    # Build all paths relative to workdir.
    perf_stats_file = workdir / "cardano-db-sync/db_sync_performance_stats.json"
    node_log_file = workdir / "node_sync.log"
    db_sync_log_file = workdir / "db_sync.log"
    epoch_sync_times_file = workdir / "cardano-db-sync/epoch_sync_times_dump.json"
    postgres_log_file = pg_dir / "postgres.log"
    monitor_log_file = workdir / "monitor.log"
    monitor_stderr_log_file = workdir / "monitor_stderr.log"
    oom_log_file = workdir / "oom.log"

    node_archive_name = "cardano_node_logs.zip"
    db_sync_archive_name = "cardano_db_sync_logs.zip"
    sync_data_archive_name = "epoch_sync_times_dump.zip"
    perf_stats_archive_name = "db_sync_perf_stats.zip"
    monitor_archive_name = "monitor.zip"

    chart_name = "full_sync_stats_chart.png"

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
        postgres_log_file=postgres_log_file,
        monitor_log_file=monitor_log_file,
        monitor_stderr_log_file=monitor_stderr_log_file,
        oom_log_file=oom_log_file,
        node_archive_name=node_archive_name,
        db_sync_archive_name=db_sync_archive_name,
        sync_data_archive_name=sync_data_archive_name,
        perf_stats_archive_name=perf_stats_archive_name,
        monitor_archive_name=monitor_archive_name,
        chart_name=chart_name,
        node_pr=node_pr,
        node_branch=node_branch,
        node_version=node_version,
        db_sync_branch=db_sync_branch,
        db_sync_version=db_sync_version,
    )
