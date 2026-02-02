import argparse
import datetime
import logging
import os
import pathlib as pl
import sys
import typing as tp
from collections import OrderedDict

from sync_tests.tests import full_sync_entries
from sync_tests.utils import artifacts
from sync_tests.utils import db_sync
from sync_tests.utils import helpers
from sync_tests.utils.db_sync import metrics_extractor as db_sync_metrics_extractor
from sync_tests.utils.logs import color_logger
from sync_tests.utils.logs import log_analyzer

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
    db_sync_start_era = args.db_sync_start_era

    # Create test_workdir for all logs and test data (but not artifacts)
    root_dir = pl.Path.cwd()
    test_workdir = root_dir / "test_workdir"
    test_workdir.mkdir(exist_ok=True)
    LOGGER.info(f"Using test_workdir for logs: {test_workdir}")

    # Create DbSyncConfig for all db-sync operations - use test_workdir for logs
    config = db_sync.create_db_sync_config(env=env, workdir=test_workdir)

    # Create and clear db-sync logfile early so it's ready when db-sync starts
    # This ensures the logfile exists and is empty before db-sync setup begins
    config.db_sync_log_file.parent.mkdir(parents=True, exist_ok=True)
    with open(config.db_sync_log_file, "w") as f:
        f.write("")
    LOGGER.info(f"Created and cleared db-sync logfile: {config.db_sync_log_file}")
    LOGGER.info(f"DB sync will write logs to: {config.db_sync_log_file}")
    LOGGER.info(f"Monitor db-sync logs with: tail -f {config.db_sync_log_file}")

    node_pr = helpers.get_arg_value(args=args, key="node_pr", default="")
    LOGGER.info(f"Node PR number: {node_pr}")

    node_branch = helpers.get_arg_value(args=args, key="node_branch", default="")
    LOGGER.info(f"Node branch: {node_branch}")

    node_revision = helpers.get_arg_value(args=args, key="node_revision")
    if not node_revision and not helpers.get_arg_value(args=args, key="db_sync_revision"):
        msg = "Provide --node-revision and/or --db-sync-revision"
        raise ValueError(msg)
    if node_revision:
        LOGGER.info(f"Node revision: {node_revision}")

    db_branch = helpers.get_arg_value(args=args, key="db_sync_branch", default="")
    LOGGER.info(f"DB sync branch: {db_branch}")

    # `helpers.get_arg_value` can return None if the arg is missing; keep this as a string.
    db_start_options = (
        helpers.get_arg_value(args=args, key="db_sync_start_options", default="") or ""
    )

    db_sync_revision = helpers.get_arg_value(args=args, key="db_sync_revision")
    db_sync_rev_with_opts = ""
    if db_sync_revision:
        db_sync_rev_with_opts = db_sync_revision + (
            " " + db_start_options if db_start_options else ""
        )
        LOGGER.info(f"DB sync revision: {db_sync_rev_with_opts}")

    # cardano-node setup keeps root_dir to preserve socket path compatibility.
    conf_dir = pl.Path.cwd()
    base_dir = pl.Path.cwd()
    node_result = None
    node_started = False
    if node_revision:
        node_result = full_sync_entries.run_node_sync(
            env=env,
            node_revision=node_revision,
            config=config,
            base_dir=base_dir,
            conf_dir=conf_dir,
            start_era=db_sync_start_era,
        )
        node_started = True

    if not db_sync_revision:
        LOGGER.info("--- Skipping db-sync (db-sync revision not set)")
        if node_started:
            helpers.manage_process(proc_name="cardano-node", action="terminate")
        return

    if not node_result:
        node_socket_candidates = [
            args.node_socket_path,
            os.environ.get("CARDANO_NODE_SOCKET_PATH"),
            str(base_dir / "db" / "node.socket"),
            str(base_dir / "test_workdir" / "db" / "node.socket"),
            str(base_dir / "test_workdir_node_only" / "db" / "node.socket"),
        ]
        node_socket_path = next(
            (p for p in node_socket_candidates if p and pl.Path(p).exists()),
            None,
        )
        if not node_socket_path:
            msg = (
                "Node socket not found. Provide --node-socket-path, set "
                "CARDANO_NODE_SOCKET_PATH, or ensure a default socket path exists."
            )
            raise RuntimeError(msg)
        helpers.export_env_var("CARDANO_NODE_SOCKET_PATH", node_socket_path)
        LOGGER.info(f"Using existing node socket: {node_socket_path}")

    db_result = full_sync_entries.run_db_sync(
        env=env,
        db_sync_revision=db_sync_revision,
        db_start_options=db_start_options,
        config=config,
        node_logfile_path=node_result.node_logfile_path if node_result else config.node_log_file,
    )

    db_sync_version = db_result.db_sync_version
    db_sync_git_rev = db_result.db_sync_git_rev
    db_full_sync_time_in_secs = db_result.db_full_sync_time_in_secs
    perf_stats = db_result.perf_stats
    db_sync_tip = db_result.db_sync_tip
    epoch_no = db_sync_tip.epoch_no
    block_no = db_sync_tip.block_no
    slot_no = db_sync_tip.slot_no
    end_test_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")

    LOGGER.info("--- Summary & Artifacts uploading")
    db_sync_progress = db_sync.get_db_sync_progress(config)
    LOGGER.info(f"FINAL db-sync progress: {db_sync_progress}, epoch: {epoch_no}, block: {block_no}")
    LOGGER.info(f"TOTAL sync time [sec]: {db_full_sync_time_in_secs}")

    # shut down services
    helpers.manage_process(proc_name="cardano-db-sync", action="terminate")
    if node_started:
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
    test_data["node_version"] = node_revision
    test_data["db_sync_branch"] = db_branch
    test_data["db_version"] = db_sync_rev_with_opts
    test_data["node_cli_version"] = node_result.cli_version if node_result else None
    test_data["node_git_revision"] = node_result.cli_git_rev if node_result else None
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

    # Extract log-based metrics from db-sync log file
    LOGGER.info("Extracting log-based metrics from db-sync log file...")
    try:
        db_sync_log_metrics = db_sync_metrics_extractor.get_db_sync_data_from_logs(
            config.db_sync_log_file
        )
        test_data["epoch_timings"] = db_sync_log_metrics["epoch_timings"]
        test_data["block_insertion_rates"] = db_sync_log_metrics["block_insertions"]
        test_data["epoch_details"] = db_sync_log_metrics["epoch_details"]
        LOGGER.info(f"Extracted metrics for {len(db_sync_log_metrics['epoch_timings'])} epochs")
    except Exception as e:
        LOGGER.warning(f"Failed to extract log-based metrics: {e}")
        test_data["epoch_timings"] = {}
        test_data["block_insertion_rates"] = []
        test_data["epoch_details"] = {}

    helpers.write_json_to_file(test_results_file, test_data)

    # Artifact handling: logs remain in test_workdir/ for debugging
    # Only create zip files if Build kite is available (for CI artifact upload)
    # Use actual node logfile path (timestamped in test_workdir)
    node_logfile_path = node_result.node_logfile_path if node_result else config.node_log_file

    # Check if we're in CI (Buildkite available)
    is_ci = artifacts.is_ci_environment()

    if is_ci and node_result:
        # In CI: create zip files for Buildkite upload
        artifact_dir = root_dir
        LOGGER.info("CI environment detected - creating zip files for Buildkite upload")

        node_archive_path = artifact_dir / config.node_archive_name
        db_sync_archive_path = artifact_dir / config.db_sync_archive_name
        helpers.zip_file(str(node_archive_path), node_logfile_path)
        helpers.zip_file(str(db_sync_archive_path), config.db_sync_log_file)

        # Upload zipped logs to Build kite (logs remain in test_workdir/ for debugging)
        db_sync.upload_artifact(str(node_archive_path))
        db_sync.upload_artifact(str(db_sync_archive_path))
        # test_results_file is already in test_workdir, upload it to Build kite
        db_sync.upload_artifact(str(test_results_file))
    else:
        # Local run: skip zipping logs (they're already accessible in test_workdir/)
        LOGGER.info("Local environment detected - logs remain in test_workdir/ for debugging")
        LOGGER.info(f"Node logs: {node_logfile_path}")
        LOGGER.info(f"DB sync logs: {config.db_sync_log_file}")
        LOGGER.info(f"Test results: {test_results_file}")

    # AWS/S3 uploads removed; results remain local in the test_workdir

    # create and upload a compressed node db archive (only in CI)
    if env != "mainnet" and is_ci and node_result:
        node_db = db_sync.create_node_database_archive(config)
        # Move the node_db archive to artifact_dir if needed
        artifact_dir = root_dir
        if node_db.parent != artifact_dir:
            node_db_target = artifact_dir / node_db.name
            node_db.rename(node_db_target)
            db_sync.upload_artifact(str(node_db_target))
        else:
            db_sync.upload_artifact(str(node_db))

    # search db-sync log for issues
    log_analyzer.check_db_sync_logs(log_file=config.db_sync_log_file)


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
        required=False,
        help="Desired cardano-node revision - cardano-node tag or branch",
    )
    parser.add_argument(
        "-dv",
        "--db-sync-revision",
        required=False,
        help="Desired db-sync revision - db-sync tag or branch",
    )
    parser.add_argument(
        "--node-socket-path",
        help="Path to an existing node socket when running db-sync without node sync",
    )
    parser.add_argument(
        "--db-sync-start-era",
        default="shelley",
        choices=("shelley", "allegra", "mary", "alonzo", "babbage", "conway"),
        help="Minimum node era before starting db-sync (default: shelley)",
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
