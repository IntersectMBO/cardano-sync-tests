import os
import sys
import json
import argparse

from collections import OrderedDict
from datetime import timedelta

sys.path.append(os.getcwd())

import sync_tests.utils.utils as utils
import sync_tests.utils.aws_db_utils as aws_db_utils
import sync_tests.utils.utils_db_sync as utils_db_sync
import sync_tests.utils.gitpython_utils as git_utils

from datetime import datetime
from datetime import timedelta

TEST_RESULTS = 'db_sync_iohk_snapshot_restoration_test_results.json'


def upload_snapshot_restoration_results_to_aws(env):
    print("--- Write IOHK snapshot restoration results to AWS Database")
    with open(TEST_RESULTS, "r") as json_file:
        sync_test_results_dict = json.load(json_file)

    test_summary_table = env + '_db_sync_snapshot_restoration'
    test_id = str(int(aws_db_utils.get_last_identifier(test_summary_table).split("_")[-1]) + 1)
    identifier = env + "_restoration_" + test_id
    sync_test_results_dict["identifier"] = identifier

    print(f"  ==== Write test values into the {test_summary_table} DB table:")
    col_to_insert = list(sync_test_results_dict.keys())
    val_to_insert = list(sync_test_results_dict.values())

    if not aws_db_utils.add_single_row_into_db(test_summary_table, col_to_insert, val_to_insert):
        print(f"col_to_insert: {col_to_insert}")
        print(f"val_to_insert: {val_to_insert}")
        exit(1)


def main():

    print("--- Db-sync restoration from IOHK official snapshot")
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

    node_version_from_gh_action = utils.get_arg_value(args=args, key="node_version_gh_action", default="")
    print(f"Node version: {node_version_from_gh_action}")

    db_branch = utils.get_arg_value(args=args, key="db_sync_branch", default="")
    print(f"DB sync branch: {db_branch}")

    db_sync_version_from_gh_action = utils.get_arg_value(args=args, key="db_sync_version_gh_action", default="")
    print(f"DB sync version: {db_sync_version_from_gh_action}")

    snapshot_url = utils_db_sync.get_latest_snapshot_url(env, args)
    print(f"Snapshot url: {snapshot_url}")

    # cardano-node setup
    NODE_DIR=git_utils.clone_repo('cardano-node', node_branch)
    os.chdir(NODE_DIR)
    utils.execute_command("nix build -v .#cardano-node -o cardano-node-bin")
    utils.execute_command("nix build -v .#cardano-cli -o cardano-cli-bin")
    print("--- Node setup")
    utils_db_sync.copy_node_executables(build_method="nix")
    utils_db_sync.get_node_config_files(env)
    utils_db_sync.set_node_socket_path_env_var_in_cwd()
    cli_version, cli_git_rev = utils_db_sync.get_node_version()
    utils_db_sync.download_and_extract_node_snapshot(env)
    utils_db_sync.start_node_in_cwd(env)
    print("--- Node startup", flush=True)
    utils_db_sync.print_file(utils_db_sync.NODE_LOG_FILE, 80)
    node_sync_time_in_secs = utils_db_sync.wait_for_node_to_sync(env)

    # cardano-db sync setup
    print("--- Db sync setup")
    os.chdir(utils_db_sync.ROOT_TEST_PATH)
    DB_SYNC_DIR = git_utils.clone_repo('cardano-db-sync', db_branch)
    os.chdir(DB_SYNC_DIR)
    utils_db_sync.setup_postgres()
    utils_db_sync.create_pgpass_file(env)
    utils_db_sync.create_database()
    utils_db_sync.list_databases()
    utils.execute_command("nix build .#cardano-db-sync -o db-sync-node")
    utils.execute_command("nix build .#cardano-db-tool -o db-sync-tool")
    print("--- Download and check db-sync snapshot", flush=True)
    utils_db_sync.copy_db_sync_executables(build_method="nix")
    snapshot_name = utils_db_sync.download_db_sync_snapshot(snapshot_url)
    expected_snapshot_sha_256_sum = utils_db_sync.get_snapshot_sha_256_sum(snapshot_url)
    actual_snapshot_sha_256_sum = utils_db_sync.get_file_sha_256_sum(snapshot_name)
    assert expected_snapshot_sha_256_sum == actual_snapshot_sha_256_sum, "Incorrect sha 256 sum"

    # restore snapshot
    print("--- Snapshot restoration")
    restoration_time = utils_db_sync.restore_db_sync_from_snapshot(env, snapshot_name, remove_ledger_dir="no")
    print(f"Restoration time [sec]: {restoration_time}")
    snapshot_epoch_no, snapshot_block_no, snapshot_slot_no = utils_db_sync.get_db_sync_tip(env)
    print(f"db-sync tip after restoration: epoch: {snapshot_epoch_no}, block: {snapshot_block_no}, slot: {snapshot_slot_no}")

    # start db-sync
    print("--- Db sync start")
    utils_db_sync.start_db_sync(env, start_args="", first_start="True")
    utils_db_sync.print_file(utils_db_sync.DB_SYNC_LOG_FILE, 30)
    db_sync_version, db_sync_git_rev = utils_db_sync.get_db_sync_version()
    db_full_sync_time_in_secs = utils_db_sync.wait_for_db_to_sync(env)
    end_test_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    wait_time = 30
    print(f"Waiting for additional {wait_time} minutes to continue syncying...")
    utils_db_sync.wait(wait_time * utils_db_sync.ONE_MINUTE)
    utils_db_sync.print_file(utils_db_sync.DB_SYNC_LOG_FILE, 60)
    epoch_no, block_no, slot_no = utils_db_sync.get_db_sync_tip(env)

    # shut down services
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
    test_data["node_cli_version"] = cli_version
    test_data["node_git_revision"] = cli_git_rev
    test_data["db_sync_version"] = db_sync_version
    test_data["db_sync_git_rev"] = db_sync_git_rev
    test_data["start_test_time"] = start_test_time
    test_data["end_test_time"] = end_test_time
    test_data["node_total_sync_time_in_sec"] = node_sync_time_in_secs
    test_data["node_total_sync_time_in_h_m_s"] = str(timedelta(seconds=int(node_sync_time_in_secs)))
    test_data["db_total_sync_time_in_sec"] = db_full_sync_time_in_secs
    test_data["db_total_sync_time_in_h_m_s"] = str(timedelta(seconds=db_full_sync_time_in_secs))
    test_data["snapshot_url"] = snapshot_url
    test_data["snapshot_name"] = snapshot_name
    test_data["snapshot_epoch_no"] = snapshot_epoch_no
    test_data["snapshot_block_no"] = snapshot_block_no
    test_data["snapshot_slot_no"] = snapshot_slot_no
    test_data["last_synced_epoch_no"] = epoch_no
    test_data["last_synced_block_no"] = block_no
    test_data["last_synced_slot_no"] = slot_no
    last_perf_stats_data_point = utils_db_sync.get_last_perf_stats_point()
    test_data["cpu_percent_usage"] = last_perf_stats_data_point["cpu_percent_usage"]
    test_data["total_rss_memory_usage_in_B"] = last_perf_stats_data_point["rss_mem_usage"]
    test_data["total_database_size"] = utils_db_sync.get_total_db_size(env)
    test_data["rollbacks"] = utils_db_sync.are_rollbacks_present_in_db_sync_logs(utils_db_sync.DB_SYNC_LOG_FILE)
    test_data["errors"] = utils_db_sync.are_errors_present_in_db_sync_logs(utils_db_sync.DB_SYNC_LOG_FILE)

    utils_db_sync.write_data_as_json_to_file(TEST_RESULTS, test_data)
    utils_db_sync.write_data_as_json_to_file(utils_db_sync.DB_SYNC_PERF_STATS_FILE, utils_db_sync.db_sync_perf_stats)
    utils_db_sync.export_epoch_sync_times_from_db(env, utils_db_sync.EPOCH_SYNC_TIMES_FILE, snapshot_epoch_no)

    utils_db_sync.print_file(TEST_RESULTS)

    # compress artifacts
    utils.zip_file(utils_db_sync.NODE_ARCHIVE_NAME, utils_db_sync.NODE_LOG_FILE)
    utils.zip_file(utils_db_sync.DB_SYNC_ARCHIVE_NAME, utils_db_sync.DB_SYNC_LOG_FILE)
    utils.zip_file(utils_db_sync.SYNC_DATA_ARCHIVE_NAME, utils_db_sync.EPOCH_SYNC_TIMES_FILE)
    utils.zip_file(utils_db_sync.PERF_STATS_ARCHIVE_NAME, utils_db_sync.DB_SYNC_PERF_STATS_FILE)

    # upload artifacts
    utils_db_sync.upload_artifact(utils_db_sync.NODE_ARCHIVE_NAME)
    utils_db_sync.upload_artifact(utils_db_sync.DB_SYNC_ARCHIVE_NAME)
    utils_db_sync.upload_artifact(utils_db_sync.SYNC_DATA_ARCHIVE_NAME)
    utils_db_sync.upload_artifact(utils_db_sync.PERF_STATS_ARCHIVE_NAME)
    utils_db_sync.upload_artifact(TEST_RESULTS)

    # send data to aws database
    upload_snapshot_restoration_results_to_aws(env)

    # search db-sync log for issues
    print("--- Summary: Rollbacks, errors and other isssues")

    log_errors = utils_db_sync.are_errors_present_in_db_sync_logs(utils_db_sync.DB_SYNC_LOG_FILE)
    utils_db_sync.print_color_log(utils_db_sync.sh_colors.WARNING, f"Are errors present: {log_errors}")

    rollbacks = utils_db_sync.are_rollbacks_present_in_db_sync_logs(utils_db_sync.DB_SYNC_LOG_FILE)
    utils_db_sync.print_color_log(utils_db_sync.sh_colors.WARNING, f"Are rollbacks present: {rollbacks}")

    failed_rollbacks = utils_db_sync.is_string_present_in_file(utils_db_sync.DB_SYNC_LOG_FILE, "Rollback failed")
    utils_db_sync.print_color_log(utils_db_sync.sh_colors.WARNING, f"Are failed rollbacks present: {failed_rollbacks}")

    corrupted_ledger_files = utils_db_sync.is_string_present_in_file(utils_db_sync.DB_SYNC_LOG_FILE, "Failed to parse ledger state")
    utils_db_sync.print_color_log(utils_db_sync.sh_colors.WARNING, f"Are corrupted ledger files present: {corrupted_ledger_files}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute basic sync test\n\n")

    parser.add_argument(
        "-npr", "--node_pr", help="node pr number"
    )
    parser.add_argument(
        "-nbr", "--node_branch", help="node branch or tag"
    )
    parser.add_argument(
        "-nv", "--node_version_gh_action", help="node version - 1.33.0-rc2 (tag number) or 1.33.0 (release number - for released versions) or 1.33.0_PR2124 (for not released and not tagged runs with a specific node PR/version)"
    )
    parser.add_argument(
        "-dbr", "--db_sync_branch", help="db-sync branch"
    )
    parser.add_argument(
        "-dv", "--db_sync_version_gh_action", help="db-sync version - 12.0.0-rc2 (tag number) or 12.0.2 (release number - for released versions) or 12.0.2_PR2124 (for not released and not tagged runs with a specific db_sync PR/version)"
    )
    parser.add_argument(
        "-surl", "--snapshot_url", help="snapshot download url"
    )
    parser.add_argument(
        "-e",
        "--environment",
        help="the environment on which to run the tests - shelley_qa, testnet, staging or mainnet.",
    )

    args = parser.parse_args()

    main()
