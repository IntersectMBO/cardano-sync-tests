import dataclasses
import logging
import pathlib as pl
import shutil

from sync_tests.utils import db_sync
from sync_tests.utils import helpers
from sync_tests.utils import node
from sync_tests.utils.external import gitpython

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class NodeSyncResult:
    node_logfile_path: pl.Path
    cli_version: str
    cli_git_rev: str


@dataclasses.dataclass(frozen=True)
class DbSyncResult:
    db_sync_version: str
    db_sync_git_rev: str
    db_full_sync_time_in_secs: int
    perf_stats: list[dict]
    db_sync_tip: db_sync.DbSyncTip


def run_node_sync(
    env: str,
    node_revision: str,
    config: db_sync.DbSyncConfig,
    base_dir: pl.Path,
    conf_dir: pl.Path,
    start_era: str,
) -> NodeSyncResult:
    bin_dir = pl.Path("bin")
    bin_dir.mkdir(exist_ok=True)
    node.add_to_path(path=bin_dir)

    db_dir = base_dir / "db"
    if db_dir.exists():
        LOGGER.info(f"Removing existing node DB directory for clean start: {db_dir}")
        shutil.rmtree(db_dir, ignore_errors=True)

    node.set_node_socket_path_env_var(base_dir=base_dir)
    node.get_node_files(node_rev=node_revision, base_dir=base_dir)
    cli_version, cli_git_rev = node.get_node_version()
    node.rm_node_config_files(conf_dir=conf_dir)
    node.get_node_config_files(
        env=env,
        node_topology_type="",
        conf_dir=conf_dir,
        disable_genesis_mode_flag=False,
    )
    node.configure_node(config_file=conf_dir / "config.json")

    node_logfile_path = config.node_log_file
    node_logfile_path.parent.mkdir(parents=True, exist_ok=True)
    with open(node_logfile_path, "w") as f:
        f.write("")
    LOGGER.info(f"Cleared node logfile: {node_logfile_path}")
    LOGGER.info(f"Node will write logs to: {node_logfile_path}")
    LOGGER.info(f"Monitor node logs with: tail -f {node_logfile_path}")

    node.start_node(
        base_dir=base_dir,
        node_start_arguments=(),
        logfile_path=node_logfile_path,
    )
    node.wait_node_start(
        env=env,
        base_dir=base_dir,
        timeout_minutes=30,
        logfile_path=node_logfile_path,
    )

    LOGGER.info("--- Node startup")
    helpers.print_last_n_lines(node_logfile_path, 80)

    LOGGER.info(f"--- Waiting for node to reach {start_era} era (min_era={start_era})")
    shelley_timeout_minutes = 720 if env == "mainnet" else 60
    node.wait_for_shelley_era(
        env=env,
        base_dir=base_dir,
        timeout_minutes=shelley_timeout_minutes,
        min_era=start_era,
    )

    LOGGER.info("--- Node sync progress after reaching target era")
    tip = node.get_current_tip(env=env)
    LOGGER.warning(
        f"Node era: {tip.era}, epoch: {tip.epoch}, block: {tip.block}, "
        f"slot: {tip.slot}, syncProgress: {tip.sync_progress}"
    )

    return NodeSyncResult(
        node_logfile_path=node_logfile_path,
        cli_version=cli_version,
        cli_git_rev=cli_git_rev,
    )


def run_db_sync(
    env: str,
    db_sync_revision: str,
    db_start_options: str,
    config: db_sync.DbSyncConfig,
    node_logfile_path: pl.Path,
) -> DbSyncResult:
    db_sync_dir = gitpython.clone_repo("cardano-db-sync", db_sync_revision)
    LOGGER.info("--- Db sync setup")
    db_sync.setup_postgres(config)
    db_sync.create_pgpass_file(config)
    db_sync.create_database(config)
    helpers.execute_command("nix build -v .#cardano-db-sync -o db-sync-node", cwd=db_sync_dir)
    helpers.execute_command("nix build -v .#cardano-db-tool -o db-sync-tool", cwd=db_sync_dir)
    db_sync.copy_db_sync_executables(config, build_method="nix")

    with open(config.db_sync_log_file, "w") as f:
        f.write("")
    LOGGER.info(f"Re-cleared db-sync logfile before startup: {config.db_sync_log_file}")
    LOGGER.info("--- Db sync startup")
    LOGGER.info(f"Node logs: {node_logfile_path}")
    LOGGER.info(f"DB sync logs: {config.db_sync_log_file}")
    LOGGER.info(f"Environment: {env}")
    db_sync.start_db_sync(config, start_args=db_start_options)
    db_sync_version, db_sync_git_rev = db_sync.get_db_sync_version(config)
    helpers.print_last_n_lines(config.db_sync_log_file, 30)
    db_full_sync_time_in_secs, perf_stats = db_sync.wait_for_db_to_sync(config)
    LOGGER.info("--- Skipping DB schema and indexes validation (not required)")
    db_sync_tip = db_sync.get_db_sync_tip(config)
    if db_sync_tip is None:
        msg = "db-sync tip unavailable after full sync; check db-sync logs for errors"
        raise RuntimeError(msg)

    return DbSyncResult(
        db_sync_version=db_sync_version,
        db_sync_git_rev=db_sync_git_rev,
        db_full_sync_time_in_secs=db_full_sync_time_in_secs,
        perf_stats=perf_stats,
        db_sync_tip=db_sync_tip,
    )

