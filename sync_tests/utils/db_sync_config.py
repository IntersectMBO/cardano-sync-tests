import dataclasses
import os
import subprocess
from pathlib import Path

from sync_tests.utils import node


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
    # Keep db-sync artifacts in subdirectory, but logfiles in workdir root for consistency
    perf_stats_file = workdir / f"cardano-db-sync/db_sync_{env}_performance_stats.json"
    node_log_file = workdir / node.NODE_LOG_FILE_NAME
    db_sync_log_file = workdir / f"db_sync_{env}_logfile.log"
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
