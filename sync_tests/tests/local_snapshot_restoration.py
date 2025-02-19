import argparse
import datetime
import logging
import os
import pathlib as pl
import sys
import typing as tp
from collections import OrderedDict

sys.path.append(os.getcwd())

from sync_tests.utils import db_sync
from sync_tests.utils import helpers
from sync_tests.utils import node

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

TEST_RESULTS = f"db_sync_{db_sync.ENVIRONMENT}_local_snapshot_restoration_test_results.json"
DB_SYNC_RESTORATION_ARCHIVE = f"cardano_db_sync_{db_sync.ENVIRONMENT}_restoration.zip"


def main() -> int:
    if helpers.get_arg_value(args=args, key="run_only_sync_test", default=False) == "true":
        print("--- Skipping Db sync snapshot restoration")
        return 0

    print("--- Db sync snapshot restoration")

    # system and software versions details
    platform_system, platform_release, platform_version = helpers.get_os_type()
    print(f"Platform: {platform_system, platform_release, platform_version}")

    start_test_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
    print(f"Test start time: {start_test_time}")

    env = helpers.get_arg_value(args=args, key="environment")
    print(f"Environment: {env}")

    node_pr = helpers.get_arg_value(args=args, key="node_pr", default="")
    print(f"Node PR number: {node_pr}")

    node_branch = helpers.get_arg_value(args=args, key="node_branch", default="")
    print(f"Node branch: {node_branch}")

    node_version_from_gh_action = helpers.get_arg_value(
        args=args, key="node_version_gh_action", default=""
    )
    print(f"Node version: {node_version_from_gh_action}")

    db_branch = helpers.get_arg_value(args=args, key="db_sync_branch", default="")
    print(f"DB sync branch: {db_branch}")

    db_sync_version_from_gh_action = helpers.get_arg_value(
        args=args, key="db_sync_version_gh_action", default=""
    )
    print(f"DB sync version: {db_sync_version_from_gh_action}")

    # database setup
    print("--- Local snapshot restoration - postgres and database setup")
    db_sync.setup_postgres(pg_port="5433")
    db_sync.create_pgpass_file(env)
    db_sync.create_database()

    # snapshot restoration
    os.chdir(db_sync.ROOT_TEST_PATH)
    os.chdir(pl.Path.cwd() / "cardano-db-sync")
    snapshot_file = db_sync.get_buildkite_meta_data("snapshot_file")
    print("--- Local snapshot restoration - restoration process")
    print(f"Snapshot file from key-store: {snapshot_file}")
    restoration_time = db_sync.restore_db_sync_from_snapshot(env, snapshot_file)
    print(f"Restoration time [sec]: {restoration_time}")
    db_sync_tip = db_sync.get_db_sync_tip(env)
    assert db_sync_tip is not None  # TODO: refactor
    snapshot_epoch_no, snapshot_block_no, snapshot_slot_no = db_sync_tip
    print(
        f"db-sync tip after snapshot restoration: epoch: {snapshot_epoch_no}, "
        f"block: {snapshot_block_no}, slot: {snapshot_slot_no}"
    )

    # start node
    print("--- Node startup after snapshot restoration")
    # cardano-node setup
    conf_dir = pl.Path.cwd()
    base_dir = pl.Path.cwd()
    bin_dir = pl.Path("bin")
    bin_dir.mkdir(exist_ok=True)
    node.add_to_path(path=bin_dir)

    node.set_node_socket_path_env_var(base_dir=base_dir)
    node.get_node_files(node_rev=node_version_from_gh_action)
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
    node.wait_node_start(env=env, timeout_minutes=10)
    db_sync.print_file(db_sync.NODE_LOG_FILE, 80)
    node.wait_for_node_to_sync(env=env, base_dir=base_dir)

    # start db-sync
    print("--- Db-sync startup after snapshot restoration")
    os.chdir(db_sync.ROOT_TEST_PATH)
    os.chdir(pl.Path.cwd() / "cardano-db-sync")
    db_sync.export_env_var("PGPORT", "5433")
    db_sync.start_db_sync(env, start_args="", first_start="False")
    db_sync.print_file(db_sync.DB_SYNC_LOG_FILE, 20)
    db_sync.wait(db_sync.ONE_MINUTE)
    db_sync_version, db_sync_git_rev = db_sync.get_db_sync_version()
    db_full_sync_time_in_secs = db_sync.wait_for_db_to_sync(env)
    end_test_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
    wait_time = 20
    print(f"Waiting for additional {wait_time} minutes to continue syncying...")
    db_sync.wait(wait_time * db_sync.ONE_MINUTE)
    db_sync_tip = db_sync.get_db_sync_tip(env)
    assert db_sync_tip is not None  # TODO: refactor
    epoch_no, block_no, slot_no = db_sync_tip
    print(f"Test end time: {end_test_time}")
    db_sync.print_file(db_sync.DB_SYNC_LOG_FILE, 60)

    # stop cardano-node and cardano-db-sync
    print("--- Stop cardano services")
    helpers.manage_process(proc_name="cardano-db-sync", action="terminate")
    helpers.manage_process(proc_name="cardano-node", action="terminate")

    # export test data as a json file
    print("--- Gathering end results")
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
    test_data["db_sync_version"] = db_sync_version
    test_data["db_sync_git_rev"] = db_sync_git_rev
    test_data["start_test_time"] = start_test_time
    test_data["end_test_time"] = end_test_time
    test_data["db_total_sync_time_in_sec"] = db_full_sync_time_in_secs
    test_data["db_total_sync_time_in_h_m_s"] = str(
        datetime.timedelta(seconds=int(db_full_sync_time_in_secs))
    )
    test_data["snapshot_name"] = snapshot_file
    test_data["snapshot_size_in_mb"] = db_sync.get_file_size(snapshot_file)
    test_data["restoration_time"] = restoration_time
    test_data["snapshot_epoch_no"] = snapshot_epoch_no
    test_data["snapshot_block_no"] = snapshot_block_no
    test_data["snapshot_slot_no"] = snapshot_slot_no
    test_data["last_synced_epoch_no"] = epoch_no
    test_data["last_synced_block_no"] = block_no
    test_data["last_synced_slot_no"] = slot_no
    test_data["total_database_size"] = db_sync.get_total_db_size(env)
    test_data["rollbacks"] = db_sync.is_string_present_in_file(
        db_sync.DB_SYNC_LOG_FILE, "rolling back to"
    )
    test_data["errors"] = db_sync.are_errors_present_in_db_sync_logs(db_sync.DB_SYNC_LOG_FILE)

    db_sync.write_data_as_json_to_file(TEST_RESULTS, test_data)
    db_sync.print_file(TEST_RESULTS)

    # compress & upload artifacts
    helpers.zip_file(DB_SYNC_RESTORATION_ARCHIVE, db_sync.DB_SYNC_LOG_FILE)
    db_sync.upload_artifact(DB_SYNC_RESTORATION_ARCHIVE)
    db_sync.upload_artifact(TEST_RESULTS)

    # search db-sync log for issues
    print("--- Summary: Rollbacks, errors and other isssues")

    log_errors = db_sync.are_errors_present_in_db_sync_logs(db_sync.DB_SYNC_LOG_FILE)
    db_sync.print_color_log(db_sync.sh_colors.WARNING, f"Are errors present: {log_errors}")

    rollbacks = db_sync.are_rollbacks_present_in_db_sync_logs(db_sync.DB_SYNC_LOG_FILE)
    db_sync.print_color_log(db_sync.sh_colors.WARNING, f"Are rollbacks present: {rollbacks}")

    failed_rollbacks = db_sync.is_string_present_in_file(
        db_sync.DB_SYNC_LOG_FILE, "Rollback failed"
    )
    db_sync.print_color_log(
        db_sync.sh_colors.WARNING,
        f"Are failed rollbacks present: {failed_rollbacks}",
    )

    corrupted_ledger_files = db_sync.is_string_present_in_file(
        db_sync.DB_SYNC_LOG_FILE, "Failed to parse ledger state"
    )
    db_sync.print_color_log(
        db_sync.sh_colors.WARNING,
        f"Are corrupted ledger files present: {corrupted_ledger_files}",
    )
    return 0


if __name__ == "__main__":

    def hyphenated(string: str) -> str:
        return "--" + string

    parser = argparse.ArgumentParser(description="Execute basic sync test\n\n")

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
    parser.add_argument("-dbr", "--db_sync_branch", help="db-sync branch")
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
    parser.add_argument("-rosc", "--run_only_sync_test", help="should run only sync test ?")

    args = parser.parse_args()

    main()
