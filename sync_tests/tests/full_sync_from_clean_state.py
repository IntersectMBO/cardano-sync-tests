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

TEST_RESULTS = f"db_sync_{db_sync.ENVIRONMENT}_full_sync_test_results.json"
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
    db_sync.print_file(db_sync.NODE_LOG_FILE, 80)

    # cardano-db sync setup
    os.chdir(db_sync.ROOT_TEST_PATH)
    db_sync_dir = gitpython.clone_repo("cardano-db-sync", db_sync_version_from_gh_action.rstrip())
    os.chdir(db_sync_dir)
    LOGGER.info("--- Db sync setup")
    db_sync.setup_postgres()  # To login use: psql -h /path/to/postgres -p 5432 -e postgres
    db_sync.create_pgpass_file(env)
    db_sync.create_database()
    helpers.execute_command("nix build -v .#cardano-db-sync -o db-sync-node")
    helpers.execute_command("nix build -v .#cardano-db-tool -o db-sync-tool")
    db_sync.copy_db_sync_executables(build_method="nix")
    LOGGER.info("--- Db sync startup")
    db_sync.start_db_sync(env, start_args=db_start_options)
    db_sync_version, db_sync_git_rev = db_sync.get_db_sync_version()
    db_sync.print_file(db_sync.DB_SYNC_LOG_FILE, 30)
    db_full_sync_time_in_secs = db_sync.wait_for_db_to_sync(env)
    LOGGER.info("--- Db sync schema and indexes check for erors")
    db_sync.check_database(db_sync.get_db_schema, "DB schema is incorrect", EXPECTED_DB_SCHEMA)
    db_sync.check_database(db_sync.get_db_indexes, "DB indexes are incorrect", EXPECTED_DB_INDEXES)
    db_sync_tip = db_sync.get_db_sync_tip(env)
    assert db_sync_tip is not None  # TODO: refactor
    epoch_no, block_no, slot_no = db_sync_tip
    end_test_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")

    LOGGER.info("--- Summary & Artifacts uploading")
    LOGGER.info(
        f"FINAL db-sync progress: {db_sync.get_db_sync_progress(env)}, "
        f"epoch: {epoch_no}, block: {block_no}"
    )
    LOGGER.info(f"TOTAL sync time [sec]: {db_full_sync_time_in_secs}")

    # shut down services
    helpers.manage_process(proc_name="cardano-db-sync", action="terminate")
    helpers.manage_process(proc_name="cardano-node", action="terminate")

    # export test data as a json file
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
    last_perf_stats_data_point = db_sync.get_last_perf_stats_point()
    test_data["cpu_percent_usage"] = last_perf_stats_data_point["cpu_percent_usage"]
    test_data["total_rss_memory_usage_in_B"] = last_perf_stats_data_point["rss_mem_usage"]
    test_data["total_database_size"] = db_sync.get_total_db_size(env)
    test_data["rollbacks"] = log_analyzer.are_rollbacks_present_in_logs(
        log_file=db_sync.DB_SYNC_LOG_FILE
    )
    test_data["errors"] = log_analyzer.is_string_present_in_file(
        file_to_check=db_sync.DB_SYNC_LOG_FILE, search_string="db-sync-node:Error"
    )

    db_sync.write_data_as_json_to_file(TEST_RESULTS, test_data)
    db_sync.write_data_as_json_to_file(db_sync.DB_SYNC_PERF_STATS_FILE, db_sync.db_sync_perf_stats)
    db_sync.export_epoch_sync_times_from_db(env, db_sync.EPOCH_SYNC_TIMES_FILE)

    db_sync.print_file(TEST_RESULTS)

    # compress artifacts
    helpers.zip_file(db_sync.NODE_ARCHIVE_NAME, db_sync.NODE_LOG_FILE)
    helpers.zip_file(db_sync.DB_SYNC_ARCHIVE_NAME, db_sync.DB_SYNC_LOG_FILE)
    helpers.zip_file(db_sync.SYNC_DATA_ARCHIVE_NAME, db_sync.EPOCH_SYNC_TIMES_FILE)
    helpers.zip_file(db_sync.PERF_STATS_ARCHIVE_NAME, db_sync.DB_SYNC_PERF_STATS_FILE)

    # upload artifacts
    db_sync.upload_artifact(db_sync.NODE_ARCHIVE_NAME)
    db_sync.upload_artifact(db_sync.DB_SYNC_ARCHIVE_NAME)
    db_sync.upload_artifact(db_sync.SYNC_DATA_ARCHIVE_NAME)
    db_sync.upload_artifact(db_sync.PERF_STATS_ARCHIVE_NAME)
    db_sync.upload_artifact(TEST_RESULTS)

    # send results to aws database
    aws_db.upload_sync_results_to_aws(env)

    # create and upload compressed node db archive
    if env != "mainnet":
        node_db = db_sync.create_node_database_archive(env)
        db_sync.upload_artifact(node_db)

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

    parser.add_argument("-npr", "--node_pr", help="node pr number")
    parser.add_argument("-nbr", "--node_branch", help="node branch or tag")
    parser.add_argument(
        "-nv",
        "--node_version_gh_action",
        help=(
            "node version - 1.33.0-rc2 (tag number) or 1.33.0 "
            "(release number - for released versions) or 1.33.0_PR2124 "
            "(for not released and not tagged runs with a specific node PR/version)"
        ),
    )
    parser.add_argument("-dbr", "--db_sync_branch", help="db-sync branch or tag")
    parser.add_argument(
        "-dv",
        "--db_sync_version_gh_action",
        help=(
            "db-sync version - 12.0.0-rc2 (tag number) or 12.0.2 "
            "(release number - for released versions) or 12.0.2_PR2124 "
            "(for not released and not tagged runs with a specific db_sync PR/version)"
        ),
    )
    parser.add_argument(
        "-dsa",
        "--db_sync_start_options",
        type=hyphenated,
        help="db-sync start arguments: --disable-ledger, --disable-cache, --disable-epoch",
    )
    parser.add_argument(
        "-e",
        "--environment",
        help="the environment on which to run the tests - shelley_qa, testnet, staging or mainnet.",
    )

    return parser.parse_args()


def main() -> int:
    color_logger.configure_logging()
    args = get_args()
    run_test(args=args)

    return 0


if __name__ == "__main__":
    sys.exit(main())
