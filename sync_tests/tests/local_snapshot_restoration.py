import argparse
import os
import sys
from collections import OrderedDict
from datetime import datetime
from datetime import timedelta
from pathlib import Path

sys.path.append(os.getcwd())

import sync_tests.utils.db_sync as utils_db_sync
import sync_tests.utils.helpers as utils

TEST_RESULTS = (
    f"db_sync_{utils_db_sync.ENVIRONMENT}_local_snapshot_restoration_test_results.json"
)
DB_SYNC_RESTORATION_ARCHIVE = (
    f"cardano_db_sync_{utils_db_sync.ENVIRONMENT}_restoration.zip"
)


def main():
    if (
        utils.get_arg_value(args=args, key="run_only_sync_test", default=False)
        == "true"
    ):
        print("--- Skipping Db sync snapshot restoration")
        return 0

    print("--- Db sync snapshot restoration")

    # system and software versions details
    platform_system, platform_release, platform_version = utils.get_os_type()
    print(f"Platform: {platform_system, platform_release, platform_version}")

    start_test_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print(f"Test start time: {start_test_time}")

    env = utils.get_arg_value(args=args, key="environment")
    print(f"Environment: {env}")

    node_pr = utils.get_arg_value(args=args, key="node_pr", default="")
    print(f"Node PR number: {node_pr}")

    node_branch = utils.get_arg_value(args=args, key="node_branch", default="")
    print(f"Node branch: {node_branch}")

    node_version_from_gh_action = utils.get_arg_value(
        args=args, key="node_version_gh_action", default=""
    )
    print(f"Node version: {node_version_from_gh_action}")

    db_branch = utils.get_arg_value(args=args, key="db_sync_branch", default="")
    print(f"DB sync branch: {db_branch}")

    db_sync_version_from_gh_action = utils.get_arg_value(
        args=args, key="db_sync_version_gh_action", default=""
    )
    print(f"DB sync version: {db_sync_version_from_gh_action}")

    # database setup
    print("--- Local snapshot restoration - postgres and database setup")
    utils_db_sync.setup_postgres(pg_port="5433")
    utils_db_sync.create_pgpass_file(env)
    utils_db_sync.create_database()

    # snapshot restoration
    os.chdir(utils_db_sync.ROOT_TEST_PATH)
    os.chdir(Path.cwd() / "cardano-db-sync")
    snapshot_file = utils_db_sync.get_buildkite_meta_data("snapshot_file")
    print("--- Local snapshot restoration - restoration process")
    print(f"Snapshot file from key-store: {snapshot_file}")
    restoration_time = utils_db_sync.restore_db_sync_from_snapshot(env, snapshot_file)
    print(f"Restoration time [sec]: {restoration_time}")
    snapshot_epoch_no, snapshot_block_no, snapshot_slot_no = (
        utils_db_sync.get_db_sync_tip(env)
    )
    print(
        f"db-sync tip after snapshot restoration: epoch: {snapshot_epoch_no}, block: {snapshot_block_no}, slot: {snapshot_slot_no}"
    )

    # start node
    print("--- Node startup after snapshot restoration")
    os.chdir(utils_db_sync.ROOT_TEST_PATH)
    os.chdir(Path.cwd() / "cardano-node")
    utils_db_sync.set_node_socket_path_env_var_in_cwd()
    utils_db_sync.start_node_in_cwd(env)
    utils_db_sync.print_file(utils_db_sync.NODE_LOG_FILE, 80)
    utils_db_sync.wait_for_node_to_sync(env)

    # start db-sync
    print("--- Db-sync startup after snapshot restoration")
    os.chdir(utils_db_sync.ROOT_TEST_PATH)
    os.chdir(Path.cwd() / "cardano-db-sync")
    utils_db_sync.export_env_var("PGPORT", "5433")
    utils_db_sync.start_db_sync(env, start_args="", first_start="False")
    utils_db_sync.print_file(utils_db_sync.DB_SYNC_LOG_FILE, 20)
    utils_db_sync.wait(utils_db_sync.ONE_MINUTE)
    db_sync_version, db_sync_git_rev = utils_db_sync.get_db_sync_version()
    db_full_sync_time_in_secs = utils_db_sync.wait_for_db_to_sync(env)
    end_test_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    wait_time = 20
    print(f"Waiting for additional {wait_time} minutes to continue syncying...")
    utils_db_sync.wait(wait_time * utils_db_sync.ONE_MINUTE)
    epoch_no, block_no, slot_no = utils_db_sync.get_db_sync_tip(env)
    print(f"Test end time: {end_test_time}")
    utils_db_sync.print_file(utils_db_sync.DB_SYNC_LOG_FILE, 60)

    # stop cardano-node and cardano-db-sync
    print("--- Stop cardano services")
    utils_db_sync.manage_process(proc_name="cardano-db-sync", action="terminate")
    utils_db_sync.manage_process(proc_name="cardano-node", action="terminate")

    # export test data as a json file
    print("--- Gathering end results")
    test_data = OrderedDict()
    test_data["platform_system"] = platform_system
    test_data["platform_release"] = platform_release
    test_data["platform_version"] = platform_version
    test_data["no_of_cpu_cores"] = os.cpu_count()
    test_data["total_ram_in_GB"] = utils.get_total_ram_in_GB()
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
        timedelta(seconds=int(db_full_sync_time_in_secs))
    )
    test_data["snapshot_name"] = snapshot_file
    test_data["snapshot_size_in_mb"] = utils_db_sync.get_file_size(snapshot_file)
    test_data["restoration_time"] = restoration_time
    test_data["snapshot_epoch_no"] = snapshot_epoch_no
    test_data["snapshot_block_no"] = snapshot_block_no
    test_data["snapshot_slot_no"] = snapshot_slot_no
    test_data["last_synced_epoch_no"] = epoch_no
    test_data["last_synced_block_no"] = block_no
    test_data["last_synced_slot_no"] = slot_no
    test_data["total_database_size"] = utils_db_sync.get_total_db_size(env)
    test_data["rollbacks"] = utils_db_sync.is_string_present_in_file(
        utils_db_sync.DB_SYNC_LOG_FILE, "rolling back to"
    )
    test_data["errors"] = utils_db_sync.are_errors_present_in_db_sync_logs(
        utils_db_sync.DB_SYNC_LOG_FILE
    )

    utils_db_sync.write_data_as_json_to_file(TEST_RESULTS, test_data)
    utils_db_sync.print_file(TEST_RESULTS)

    # compress & upload artifacts
    utils.zip_file(DB_SYNC_RESTORATION_ARCHIVE, utils_db_sync.DB_SYNC_LOG_FILE)
    utils_db_sync.upload_artifact(DB_SYNC_RESTORATION_ARCHIVE)
    utils_db_sync.upload_artifact(TEST_RESULTS)

    # search db-sync log for issues
    print("--- Summary: Rollbacks, errors and other isssues")

    log_errors = utils_db_sync.are_errors_present_in_db_sync_logs(
        utils_db_sync.DB_SYNC_LOG_FILE
    )
    utils_db_sync.print_color_log(
        utils_db_sync.sh_colors.WARNING, f"Are errors present: {log_errors}"
    )

    rollbacks = utils_db_sync.are_rollbacks_present_in_db_sync_logs(
        utils_db_sync.DB_SYNC_LOG_FILE
    )
    utils_db_sync.print_color_log(
        utils_db_sync.sh_colors.WARNING, f"Are rollbacks present: {rollbacks}"
    )

    failed_rollbacks = utils_db_sync.is_string_present_in_file(
        utils_db_sync.DB_SYNC_LOG_FILE, "Rollback failed"
    )
    utils_db_sync.print_color_log(
        utils_db_sync.sh_colors.WARNING,
        f"Are failed rollbacks present: {failed_rollbacks}",
    )

    corrupted_ledger_files = utils_db_sync.is_string_present_in_file(
        utils_db_sync.DB_SYNC_LOG_FILE, "Failed to parse ledger state"
    )
    utils_db_sync.print_color_log(
        utils_db_sync.sh_colors.WARNING,
        f"Are corrupted ledger files present: {corrupted_ledger_files}",
    )
    return None


if __name__ == "__main__":

    def hyphenated(string):
        return "--" + string

    parser = argparse.ArgumentParser(description="Execute basic sync test\n\n")

    parser.add_argument("-npr", "--node_pr", help="node pr number")
    parser.add_argument("-nbr", "--node_branch", help="node branch or tag")
    parser.add_argument(
        "-nv",
        "--node_version_gh_action",
        help="node version - 1.33.0-rc2 (tag number) or 1.33.0 (release number - for released versions) or 1.33.0_PR2124 (for not released and not tagged runs with a specific node PR/version)",
    )
    parser.add_argument("-dbr", "--db_sync_branch", help="db-sync branch")
    parser.add_argument(
        "-dv",
        "--db_sync_version_gh_action",
        help="db-sync version - 12.0.0-rc2 (tag number) or 12.0.2 (release number - for released versions) or 12.0.2_PR2124 (for not released and not tagged runs with a specific db_sync PR/version)",
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
    parser.add_argument(
        "-rosc", "--run_only_sync_test", help="should run only sync test ?"
    )

    args = parser.parse_args()

    main()
