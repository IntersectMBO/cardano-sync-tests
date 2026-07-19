"""Shared node and db-sync orchestration logic used by pytest fixtures."""

from __future__ import annotations

import dataclasses
import logging
import pathlib as pl
import shutil
import time

from sync_tests.utils import db_sync
from sync_tests.utils import helpers
from sync_tests.utils import node
from sync_tests.utils.external import gitpython

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class NodeRunResult:
    """Raw result returned by run_node_sync before fixture enrichment."""

    node_logfile_path: pl.Path
    cli_version: str
    cli_git_rev: str
    secs_to_start: int = 0
    sync_time_sec: int = 0
    last_slot_no: int = 0
    latest_chunk_no: int | str = 0
    era_details: dict = dataclasses.field(default_factory=dict)
    epoch_details: dict = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(frozen=True)
class DbRunResult:
    """Raw result returned by run_db_sync before fixture enrichment."""

    db_sync_version: str
    db_sync_git_rev: str
    db_full_sync_time_in_secs: int
    perf_stats: list[dict]
    db_sync_tip: db_sync.DbSyncTip | None


def run_node_sync(
    env: str,
    node_revision: str,
    node_logfile_path: pl.Path,
    base_dir: pl.Path,
    conf_dir: pl.Path,
    start_era: str,
    clean_start: bool = True,
    full_sync: bool = False,
) -> NodeRunResult:
    """Build cardano-node from revision, start it, and wait until synced.

    Args:
        env: Cardano environment (``"preview"``, ``"preprod"``, ``"mainnet"``).
        node_revision: Git tag or branch to build.
        node_logfile_path: Path where the node will write its log output.
        base_dir: Root directory for node database and socket.
        conf_dir: Directory for node configuration files.
        start_era: Minimum era the node must reach before returning
            (only used when ``full_sync=False``).
        clean_start: When ``True`` (default) the node DB directory is wiped before
            startup so the node syncs from genesis.
        full_sync: When ``True``, wait for 100 %% sync progress and collect
            era/epoch timing details (node-only pipeline). When ``False``
            (default), wait only until ``start_era`` is reached (db-sync pipeline).

    Returns:
        NodeRunResult with CLI version info, log path, and (when
        ``full_sync=True``) era/epoch/timing data.
    """
    bin_dir = pl.Path("bin")
    bin_dir.mkdir(exist_ok=True)
    node.add_to_path(path=bin_dir)

    db_dir = base_dir / "db"
    if clean_start and db_dir.exists():
        LOGGER.info("Removing existing node DB directory for clean start: %s", db_dir)
        shutil.rmtree(db_dir, ignore_errors=True)

    node.set_node_socket_path_env_var(base_dir=base_dir)
    node.get_node_files(node_rev=node_revision, base_dir=base_dir)
    cardano_cli_path = (base_dir / "bin" / "cardano-cli").resolve()
    if not cardano_cli_path.exists():
        msg = f"cardano-cli not found after build: {cardano_cli_path}"
        raise FileNotFoundError(msg)
    helpers.export_env_var("CARDANO_CLI_PATH", str(cardano_cli_path))
    cli_version, cli_git_rev = node.get_node_version()
    node.rm_node_config_files(conf_dir=conf_dir)
    node_topology_type = "non-bootstrap-peers" if env == "mainnet" else ""
    node.get_node_config_files(
        env=env,
        node_topology_type=node_topology_type,
        conf_dir=conf_dir,
        disable_genesis_mode_flag=False,
    )
    node.configure_node(
        config_file=conf_dir / "config.json",
        cli_version=cli_version,
    )

    node_logfile_path.parent.mkdir(parents=True, exist_ok=True)
    with open(node_logfile_path, "w") as f:
        f.write("")
    LOGGER.info("Cleared node logfile: %s", node_logfile_path)
    LOGGER.info("Node will write logs to: %s", node_logfile_path)
    LOGGER.info("Monitor node logs with: tail -f %s", node_logfile_path)

    node.start_node(
        base_dir=base_dir,
        node_start_arguments=(),
        logfile_path=node_logfile_path,
        conf_dir=conf_dir,
    )
    secs_to_start = node.wait_node_start(
        env=env,
        base_dir=base_dir,
        timeout_minutes=30,
        logfile_path=node_logfile_path,
    )

    # Start monitor once per workspace; skip if already running, re-start if stale.
    _workdir = node_logfile_path.parent
    db_sync.start_monitor(_workdir, env)

    LOGGER.info("--- Node startup")
    helpers.print_last_n_lines(node_logfile_path, 80)

    era_details: dict = {}
    epoch_details: dict = {}
    sync_time_sec: int = 0
    last_slot_no: int = 0
    latest_chunk_no: int | str = 0

    if full_sync:
        LOGGER.info(
            "--- Waiting for full node sync (100%%) - env=%s",
            env,
        )
        (
            sync_time_sec,
            last_slot_no,
            latest_chunk_no,
            era_details,
            epoch_details,
        ) = node.wait_for_node_to_sync(env=env, base_dir=base_dir)
        LOGGER.info(
            "--- Full sync complete: sync_time_sec=%s last_slot_no=%s latest_chunk_no=%s eras=%s",
            sync_time_sec,
            last_slot_no,
            latest_chunk_no,
            list(era_details.keys()),
        )
    else:
        LOGGER.info(
            "--- Waiting for node to reach %s era (min_era=%s)",
            start_era,
            start_era,
        )
        shelley_timeout_minutes = 720 if env == "mainnet" else 60
        phase_start = time.perf_counter()
        node.wait_for_shelley_era(
            env=env,
            base_dir=base_dir,
            timeout_minutes=shelley_timeout_minutes,
            min_era=start_era,
            logfile_path=node_logfile_path,
        )
        phase_end = time.perf_counter()
        sync_time_sec = int(phase_end - phase_start)

    LOGGER.info("--- Node sync progress after sync step")
    tip = node.get_current_tip(env=env)
    last_slot_no = tip.slot
    immutable_dir = base_dir / "db" / "immutable"
    if immutable_dir.exists():
        try:
            chunk_files = sorted(
                immutable_dir.iterdir(),
                key=lambda f: f.stat().st_mtime,
            )
            if chunk_files:
                latest_chunk_no = chunk_files[-1].stem
        except Exception:
            LOGGER.warning(
                "Failed to read immutable chunk info from %s",
                immutable_dir,
                exc_info=True,
            )
    LOGGER.info(
        "Node era: %s, epoch: %s, block: %s, slot: %s, syncProgress: %s",
        tip.era,
        tip.epoch,
        tip.block,
        tip.slot,
        tip.sync_progress,
    )

    return NodeRunResult(
        node_logfile_path=node_logfile_path,
        cli_version=cli_version,
        cli_git_rev=cli_git_rev,
        secs_to_start=secs_to_start or 0,
        sync_time_sec=sync_time_sec,
        last_slot_no=last_slot_no,
        latest_chunk_no=latest_chunk_no,
        era_details=era_details,
        epoch_details=epoch_details,
    )


def run_db_sync(
    env: str,
    db_sync_revision: str,
    db_start_options: str,
    config: db_sync.DbSyncConfig,
    node_logfile_path: pl.Path,
) -> DbRunResult:
    """Build cardano-db-sync from revision, start it, and wait for full sync.

    Args:
        env: Cardano environment (``"preview"``, ``"preprod"``, ``"mainnet"``).
        db_sync_revision: Git tag or branch to build.
        db_start_options: Extra CLI flags passed to db-sync on startup.
        config: Db-sync configuration (paths, postgres settings, etc.).
        node_logfile_path: Path to the node log file (for reference logging).

    Returns:
        DbRunResult with version info, sync time, perf stats, and tip.
    """
    cardano_cli_path = (pl.Path.cwd() / "bin" / "cardano-cli").resolve()
    if cardano_cli_path.exists():
        helpers.export_env_var("CARDANO_CLI_PATH", str(cardano_cli_path))

    db_sync_dir = gitpython.clone_repo("cardano-db-sync", db_sync_revision)
    LOGGER.info("--- Db sync setup")
    db_sync.setup_postgres(config)
    db_sync.create_pgpass_file(config)
    db_sync.create_database()
    helpers.execute_command(
        "nix build -v --accept-flake-config --print-build-logs .#cardano-db-sync -o db-sync-node",
        cwd=db_sync_dir,
    )
    helpers.execute_command(
        "nix build -v --accept-flake-config --print-build-logs .#cardano-db-tool -o db-sync-tool",
        cwd=db_sync_dir,
    )
    db_sync.copy_db_sync_executables(config, build_method="nix")

    with open(config.db_sync_log_file, "w") as f:
        f.write("")
    LOGGER.info("Re-cleared db-sync logfile before startup: %s", config.db_sync_log_file)
    LOGGER.info("--- Db sync startup")
    LOGGER.info("Node logs: %s", node_logfile_path)
    LOGGER.info("DB sync logs: %s", config.db_sync_log_file)
    LOGGER.info("Environment: %s", env)
    db_sync.start_db_sync(config, start_args=db_start_options)
    db_sync_version, db_sync_git_rev = db_sync.get_db_sync_version(config)
    helpers.print_last_n_lines(config.db_sync_log_file, 30)
    db_full_sync_time_in_secs, perf_stats = db_sync.wait_for_db_to_sync(config)
    LOGGER.info("--- Skipping DB schema and indexes validation (not required)")
    try:
        db_sync_tip = db_sync.get_db_sync_tip(config)
    except Exception:
        LOGGER.warning(
            "db-sync tip unavailable after sync (postgres may have crashed)", exc_info=True
        )
        db_sync_tip = None

    return DbRunResult(
        db_sync_version=db_sync_version,
        db_sync_git_rev=db_sync_git_rev,
        db_full_sync_time_in_secs=db_full_sync_time_in_secs,
        perf_stats=perf_stats,
        db_sync_tip=db_sync_tip,
    )


def teardown_node_and_db_sync(base_dir: pl.Path, config: db_sync.DbSyncConfig) -> None:
    """Terminate node/db-sync processes and clean up after a session.

    Shared by the snapshot-restoration test fixtures so the same teardown
    sequence isn't hand-rolled in each one.

    Args:
        base_dir: Root directory the node was started in (for DB dir removal).
        config: Db-sync configuration used to stop postgres and finalize cleanup.
    """
    helpers.manage_process(proc_name="cardano-db-sync", action="terminate")
    helpers.manage_process(proc_name="cardano-node", action="terminate")
    node.rm_node_db_dir(base_dir=base_dir)
    db_sync.stop_postgres(config)
    db_sync.finalize_session_disk_cleanup(config)
