import argparse
import datetime
import logging
import os
import pathlib as pl
import sys
import typing as tp
from collections import OrderedDict

from sync_tests.utils import aws_db
from sync_tests.utils import color_logger
from sync_tests.utils import db_sync
from sync_tests.utils import gitpython
from sync_tests.utils import helpers
from sync_tests.utils import log_analyzer
from sync_tests.utils import node

LOGGER = logging.getLogger(__name__)

sys.path.append(os.getcwd())

EXPECTED_DB_SCHEMA, EXPECTED_DB_INDEXES = helpers.load_json_files()


def run_test(args: argparse.Namespace) -> None:
    # system and software versions details
    LOGGER.info("--- Sync from clean state - setup")
    platform_system, platform_release, platform_version = helpers.get_os_type()
    LOGGER.info(f"Platform: {platform_system, platform_release, platform_version}")

    start_test_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
    LOGGER.info(f"Test start time: {start_test_time}")

    env = helpers.get_arg_value(args=args, key="environment")
    LOGGER.info(f"Environment: {env}")

    # Create DbSyncConfig for all db-sync operations
    workdir = pl.Path.cwd()
    config = db_sync.create_db_sync_config(env=env, workdir=workdir)

    node_pr = helpers.get_arg_value(args=args, key="node_pr", default="")
    LOGGER.info(f"Node PR number: {node_pr}")

    node_branch = helpers.get_arg_value(args=args, key="node_branch", default="")
    LOGGER.info(f"Node branch: {node_branch}")

    node_version_from_gh_action = helpers.get_arg_value(
        args=args, key="node_version_gh_action", default=""
    )
    LOGGER.info(f"Node version: {node_version_from_gh_action}")

    db_branch = helpers.get_arg_value(args=args, key="db_sync_branch", default="")
    LOGGER.info(f"DB sync branch: {db_branch}")

    db_start_options = helpers.get_arg_value(args=args, key="db_sync_start_options", default="")

    db_sync_version_from_gh_action = (
        helpers.get_arg_value(args=args, key="db_sync_version_gh_action", default="")
        + " "
        + db_start_options
    )
    LOGGER.info(f"DB sync version: {db_sync_version_from_gh_action}")

    # cardano-node setup
    conf_dir = pl.Path.cwd()
    base_dir = pl.Path.cwd()
    bin_dir = pl.Path("bin")
    bin_dir.mkdir(exist_ok=True)
    node.add_to_path(path=bin_dir)

    node.set_node_socket_path_env_var(base_dir=base_dir)
    node.get_node_files(node_rev=node_version_from_gh_action, base_dir=base_dir)
    cli_version, cli_git_rev = node.get_node_version()
    node.rm_node_config_files(conf_dir=conf_dir)
    # TODO: change the default to P2P when full P2P will be supported on Mainnet
    node.get_node_config_files(
        env=env,
        node_topology_type="",
        conf_dir=conf_dir,
        use_genesis_mode=False,
    )
    node.configure_node(config_file=conf_dir / "config.json")
    node.start_node(base_dir=base_dir, node_start_arguments=())
    node.wait_node_start(env=env, base_dir=base_dir, timeout_minutes=10)

    LOGGER.info("--- Node startup")
    helpers.print_last_n_lines(config.node_log_file, 80)

    # Wait for node to reach Shelley era before starting db-sync (optimized start time)
    LOGGER.info("--- Waiting for node to reach Shelley era")
    node.wait_for_shelley_era(env=env, base_dir=base_dir, timeout_minutes=60)

    # cardano-db sync setup
    db_sync_dir = gitpython.clone_repo("cardano-db-sync", db_sync_version_from_gh_action.rstrip())
    current_dir = os.getcwd()
    os.chdir(db_sync_dir)
    LOGGER.info("--- Db sync setup")
    db_sync.setup_postgres(config)  # To login use: psql -h /path/to/postgres -p 5432 -e postgres
    db_sync.create_pgpass_file(config)
    db_sync.create_database(config)
    helpers.execute_command("nix build -v .#cardano-db-sync -o db-sync-node")
    helpers.execute_command("nix build -v .#cardano-db-tool -o db-sync-tool")
    db_sync.copy_db_sync_executables(config, build_method="nix")
    LOGGER.info("--- Db sync startup")
    db_sync.start_db_sync(config, start_args=db_start_options)
    db_sync_version, db_sync_git_rev = db_sync.get_db_sync_version(config)
    helpers.print_last_n_lines(config.db_sync_log_file, 30)
    db_full_sync_time_in_secs, perf_stats = db_sync.wait_for_db_to_sync(config)
    LOGGER.info("--- Db sync schema and indexes check for erors")
    db_sync.check_database(
        lambda: db_sync.get_db_schema(config), "DB schema is incorrect", EXPECTED_DB_SCHEMA
    )
    db_sync.check_database(
        lambda: db_sync.get_db_indexes(config), "DB indexes are incorrect", EXPECTED_DB_INDEXES
    )
    db_sync_tip = db_sync.get_db_sync_tip(config)
    assert db_sync_tip is not None  # TODO: refactor
    epoch_no = db_sync_tip.epoch_no
    block_no = db_sync_tip.block_no
    slot_no = db_sync_tip.slot_no
    os.chdir(current_dir)
    end_test_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")

    LOGGER.info("--- Summary & Artifacts uploading")
    db_sync_progress = db_sync.get_db_sync_progress(config)
    LOGGER.info(
        f"FINAL db-sync progress: {db_sync_progress}, "
        f"epoch: {epoch_no}, block: {block_no}"
    )
    LOGGER.info(f"TOTAL sync time [sec]: {db_full_sync_time_in_secs}")

    # shut down services
    helpers.manage_process(proc_name="cardano-db-sync", action="terminate")
    helpers.manage_process(proc_name="cardano-node", action="terminate")

    # export test data as a json file
    test_results_file = config.workdir / f"db_sync_{config.env}_full_sync_test_results.json"
    test_data: OrderedDict[str, tp.Any] = OrderedDict()
    test_data["platform_system"] = platform_system
    test_data["platform_release"] = platform_release
    test_data["platform_version"] = platform_version
    test_data["no_of_cpu_cores"] = os.cpu_count()
    test_data["total_ram_in_GB"] = helpers.get_total_ram_in_gb()
    test_data["env"] = env
    test_data["node_pr"] = node_pr
    test_data["node_branch"] = node_branch
    test_data["node_version"] = node_version_from_gh_action
    test_data["db_sync_branch"] = db_branch
    test_data["db_version"] = db_sync_version_from_gh_action
    test_data["node_cli_version"] = cli_version
    test_data["node_git_revision"] = cli_git_rev
    test_data["db_sync_version"] = db_sync_version
    test_data["db_sync_git_rev"] = db_sync_git_rev
    test_data["start_test_time"] = start_test_time
    test_data["end_test_time"] = end_test_time
    test_data["total_sync_time_in_sec"] = db_full_sync_time_in_secs
    test_data["total_sync_time_in_h_m_s"] = str(
        datetime.timedelta(seconds=int(db_full_sync_time_in_secs))
    )
    test_data["last_synced_epoch_no"] = epoch_no
    test_data["last_synced_block_no"] = block_no
    test_data["last_synced_slot_no"] = slot_no
    last_perf_stats_data_point = db_sync.get_last_perf_stats_point(perf_stats)
    test_data["cpu_percent_usage"] = last_perf_stats_data_point.cpu_percent_usage
    test_data["total_rss_memory_usage_in_B"] = last_perf_stats_data_point.rss_mem_usage
    test_data["total_database_size"] = db_sync.get_total_db_size(config)
    test_data["rollbacks"] = log_analyzer.are_rollbacks_present_in_logs(
        log_file=config.db_sync_log_file
    )
    test_data["errors"] = log_analyzer.is_string_present_in_file(
        file_to_check=config.db_sync_log_file, search_string="db-sync-node:Error"
    )
    test_data["system_metrics"] = perf_stats

    helpers.write_json_to_file(test_results_file, test_data)

    # compress artifacts
    helpers.zip_file(config.node_archive_name, config.node_log_file)
    helpers.zip_file(config.db_sync_archive_name, config.db_sync_log_file)

    # upload artifacts
    db_sync.upload_artifact(config.node_archive_name)
    db_sync.upload_artifact(config.db_sync_archive_name)
    db_sync.upload_artifact(str(test_results_file))

    # send results to aws database
    aws_db.upload_sync_results_to_aws(config, test_results_file)

    # create and upload compressed node db archive
    if env != "mainnet":
        node_db = db_sync.create_node_database_archive(config)
        db_sync.upload_artifact(str(node_db))

    # search db-sync log for issues
    log_analyzer.check_db_sync_logs()


def get_args() -> argparse.Namespace:
    """Get command line arguments."""
    parser = argparse.ArgumentParser(description="Execute DB Sync sync test\n\n")

    def hyphenated(db_sync_start_args: str) -> str:
        start_args = db_sync_start_args.split(" ")
        final_args_string = ""

        for arg in start_args:
            final_args_string += str("--" + arg + " ")

        return final_args_string

    parser.add_argument(
        "-nv",
        "--node-revision",
        required=True,
        help=("Desired cardano-node revision - cardano-node tag or branch"),
    )
    parser.add_argument(
        "-dv",
        "--db-sync-revision",
        required=True,
        help=("Desired db-sync revision - db-sync tag or branch"),
    )
    parser.add_argument(
        "-dsa",
        "--db-sync-start-options",
        type=hyphenated,
        help="db-sync start arguments: --disable-ledger, --disable-cache, --disable-epoch",
    )
    parser.add_argument(
        "-e",
        "--environment",
        required=True,
        help="The environment on which to run the sync test - preview, preprod, mainnet",
    )

    return parser.parse_args()


def main() -> int:
    color_logger.configure_logging()
    args = get_args()
    run_test(args=args)

    return 0


if __name__ == "__main__":
    sys.exit(main())
