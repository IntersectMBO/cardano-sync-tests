import argparse
import datetime
import json
import logging
import os
import pathlib as pl
import sys

from sync_tests.utils import color_logger
from sync_tests.utils import helpers
from sync_tests.utils import metrics_extractor
from sync_tests.utils import node
from sync_tests.utils import sync_results_db

LOGGER = logging.getLogger(__name__)

RESULTS_FILE_NAME = "sync_results.json"


def run_test(args: argparse.Namespace) -> None:
    """Run the node sync test."""
    workdir: pl.Path = args.workdir
    workdir.mkdir(exist_ok=True)
    os.chdir(workdir)

    print("--- Test data information", flush=True)
    start_test_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
    helpers.print_message(type="info", message=f"Test start time: {start_test_time}")
    helpers.print_message(type="warn", message="Test parameters:")
    env = args.environment
    node_rev1 = args.node_rev1
    node_rev2 = args.node_rev2
    tag_no1 = args.tag_no1
    tag_no2 = args.tag_no2
    node_topology_type1 = args.node_topology1
    node_topology_type2 = args.node_topology2
    node_start_arguments1 = args.node_start_arguments1 or ()
    node_start_arguments2 = args.node_start_arguments2 or ()
    use_genesis_mode = args.use_genesis_mode

    node.set_node_socket_path_env_var(base_dir=workdir)

    print(f"- env: {env}")
    print(f"- tag_no1: {tag_no1}")
    print(f"- tag_no2: {tag_no2}")
    print(f"- node_rev1: {node_rev1}")
    print(f"- node_rev2: {node_rev2}")
    print(f"- node_topology_type1: {node_topology_type1}")
    print(f"- node_topology_type2: {node_topology_type2}")
    print(f"- node_start_arguments1: {node_start_arguments1}")
    print(f"- node_start_arguments2: {node_start_arguments2}")
    print(f"- use_genesis_mode: {use_genesis_mode}")

    platform_system, platform_release, platform_version = helpers.get_os_type()
    print(f"- platform: {platform_system, platform_release, platform_version}")

    print("--- Get the cardano-node files", flush=True)
    node.config_sync(
        env=env,
        base_dir=workdir,
        node_rev=node_rev1,
        node_topology_type=node_topology_type1,
        use_genesis_mode=use_genesis_mode,
    )

    helpers.print_message(type="warn", message="--- node version ")
    cli_version1, cli_git_rev1 = node.get_node_version()
    print(f"  - cardano_cli_version1: {cli_version1}")
    print(f"  - cardano_cli_git_rev1: {cli_git_rev1}")

    print(f"--- Start node sync test using node_rev1: {node_rev1}")
    helpers.print_message(
        type="ok",
        message="===================================================================================",
    )
    helpers.print_message(
        type="ok",
        message=(
            f"================== Start node sync test using node_rev1: {node_rev1} ============="
        ),
    )
    helpers.print_message(
        type="ok",
        message="===================================================================================",
    )
    print()
    sync1_rec = node.run_sync(node_start_arguments=node_start_arguments1, base_dir=workdir, env=env)
    if not sync1_rec:
        sys.exit(1)

    print(f"secs_to_start1: {sync1_rec.secs_to_start}")
    print(f"start_sync_time1: {sync1_rec.start_sync_time}")
    print(f"end_sync_time1: {sync1_rec.end_sync_time}")

    sync2_rec = None
    print(f"--- Start node using tag_no2: {tag_no2}")
    if tag_no2:
        node.delete_node_files(node_dir=workdir)
        print()
        helpers.print_message(
            type="ok",
            message="==============================================================================",
        )
        helpers.print_message(
            type="ok",
            message=(
                f"================= Start sync using node_rev2: {node_rev2} ==================="
            ),
        )
        helpers.print_message(
            type="ok",
            message="==============================================================================",
        )

        print("Get the cardano-node and cardano-cli files")
        node.config_sync(
            env=env,
            base_dir=workdir,
            node_rev=node_rev2,
            node_topology_type=node_topology_type2,
            use_genesis_mode=use_genesis_mode,
        )

        helpers.print_message(type="warn", message="node version")
        cli_version2, cli_git_rev2 = node.get_node_version()
        print(f" - cardano_cli_version2: {cli_version2}")
        print(f" - cardano_cli_git_rev2: {cli_git_rev2}")
        print()
        print(f"================ Start node using node_rev2: {node_rev2} ====================")
        sync2_rec = node.run_sync(
            node_start_arguments=node_start_arguments2, base_dir=workdir, env=env
        )
        if not sync2_rec:
            sys.exit(1)

    chain_size = helpers.get_directory_size(workdir / "db")

    print("--- Node sync test completed")

    print("Node sync test ended; Creating the `test_values_dict` dict with the test values")
    print("++++++++++++++++++++++++++++++++++++++++++++++")

    epoch_details = {}
    for epoch, epoch_data in sync1_rec.epoch_details.items():
        epoch_details[epoch] = epoch_data["sync_duration_secs"]

    logs_details_dict = metrics_extractor.get_data_from_logs(
        log_file=workdir / node.NODE_LOG_FILE_NAME
    )

    test_values_dict = {
        "env": env,
        "tag_no1": tag_no1,
        "tag_no2": tag_no2,
        "cli_version1": cli_version1,
        "cli_version2": cli_version2 if sync2_rec else None,
        "cli_git_rev1": cli_git_rev1,
        "cli_git_rev2": cli_git_rev2 if sync2_rec else None,
        "cli_revision1": cli_version1,
        "cli_revision2": cli_version2 if sync2_rec else None,
        "start_sync_time1": sync1_rec.start_sync_time,
        "end_sync_time1": sync1_rec.end_sync_time,
        "start_sync_time2": sync2_rec.start_sync_time if sync2_rec else None,
        "end_sync_time2": sync2_rec.end_sync_time if sync2_rec else None,
        "last_slot_no1": sync1_rec.last_slot_no,
        "last_slot_no2": sync2_rec.last_slot_no if sync2_rec else None,
        "start_node_secs1": sync1_rec.secs_to_start,
        "start_node_secs2": sync2_rec.secs_to_start if sync2_rec else None,
        "sync_time_seconds1": sync1_rec.sync_time_sec,
        "sync_time1": str(datetime.timedelta(seconds=sync1_rec.sync_time_sec)),
        "sync_time_seconds2": sync2_rec.sync_time_sec if sync2_rec else None,
        "sync_time2": str(datetime.timedelta(seconds=int(sync2_rec.sync_time_sec)))
        if sync2_rec
        else None,
        "total_chunks1": sync1_rec.latest_chunk_no,
        "total_chunks2": sync2_rec.latest_chunk_no if sync2_rec else None,
        "platform_system": platform_system,
        "platform_release": platform_release,
        "platform_version": platform_version,
        "chain_size_bytes": chain_size,
        "sync_duration_per_epoch": epoch_details,
        "eras_in_test": list(sync1_rec.era_details.keys()),
        "no_of_cpu_cores": os.cpu_count(),
        "total_ram_in_GB": helpers.get_total_ram_in_gb(),
        "epoch_no_d_zero": node.get_epoch_no_d_zero(env=env),
        "start_slot_no_d_zero": node.get_start_slot_no_d_zero(env=env),
        "hydra_eval_no1": node_rev1,
        "hydra_eval_no2": node_rev2,
        "node_revision1": node_rev1,
        "node_revision2": node_rev2,
        "log_values": logs_details_dict,
    }

    for era, era_data in sync1_rec.era_details.items():
        era_details = {
            f"{era}_start_time": era_data["start_time"],
            f"{era}_start_epoch": era_data["start_epoch"],
            f"{era}_slots_in_era": era_data["slots_in_era"],
            f"{era}_start_sync_time": era_data["start_sync_time"],
            f"{era}_end_sync_time": era_data["end_sync_time"],
            f"{era}_sync_duration_secs": era_data["sync_duration_secs"],
            f"{era}_sync_speed_sps": era_data["sync_speed_sps"],
        }
        test_values_dict.update(era_details)

    # Store sync results in the database
    sync_data = {
        "sync_run_data": test_values_dict,
        "era_details": sync1_rec.era_details,
        "epoch_data": epoch_details,
        "system_metrics": logs_details_dict,
    }

    print("--- Store tests results in database")
    sync_results_db.store_sync_results(sync_data=sync_data)

    print("--- Write tests results to file")
    current_directory = pl.Path.cwd()
    print(f"current_directory: {current_directory}")
    print(f"Write the test values to the {current_directory / RESULTS_FILE_NAME} file")
    with open(RESULTS_FILE_NAME, "w") as results_file:
        json.dump(test_values_dict, results_file, indent=2)


def get_args() -> argparse.Namespace:
    """Get command line arguments."""
    parser = argparse.ArgumentParser(description="Run Cardano Node sync test\n\n")

    parser.add_argument(
        "-e",
        "--environment",
        required=True,
        help="The environment on which to run the sync test - preview, preprod, mainnet",
    )
    parser.add_argument(
        "-w",
        "--workdir",
        type=lambda p: pl.Path(p).absolute(),
        default=".",
        help="The working directory where the test will be run",
    )
    parser.add_argument(
        "-r1",
        "--node-rev1",
        required=True,
        help=(
            "Desired cardano-node revision - cardano-node tag or branch "
            "(used for initial sync, from clean state)"
        ),
    )
    parser.add_argument(
        "-r2",
        "--node-rev2",
        help=(
            "Desired cardano-node revision - cardano-node tag or branch "
            "(used for final sync, from existing state)"
        ),
    )
    parser.add_argument(
        "-t1",
        "--tag-no1",
        required=True,
        help="The 'tag_no1' label as it will be shown in the db/visuals",
    )
    parser.add_argument(
        "-t2",
        "--tag-no2",
        help="The 'tag_no2' label as it will be shown in the db/visuals",
    )
    parser.add_argument(
        "-n1",
        "--node-topology1",
        default="non-bootstrap-peers",
        help=(
            "Type of node topology used for the initial sync - "
            "legacy, non-bootstrap-peers, bootstrap-peers"
        ),
    )
    parser.add_argument(
        "-n2",
        "--node-topology2",
        default="non-bootstrap-peers",
        help=(
            "Type of node topology used for final sync (after restart) - "
            "legacy, non-bootstrap-peers, bootstrap-peers"
        ),
    )
    parser.add_argument(
        "-a1",
        "--node-start-arguments1",
        nargs="+",
        type=str,
        help="Arguments to be passed when starting the node from clean state (first tag_no)",
    )
    parser.add_argument(
        "-a2",
        "--node-start-arguments2",
        nargs="+",
        type=str,
        help="Arguments to be passed when starting the node from existing state (second tag_no)",
    )
    parser.add_argument(
        "-g",
        "--use-genesis-mode",
        action="store_true",
        default=False,
        help="Use genesis mode",
    )
    parser.add_argument(
        "-s",
        "--store_sync_results",
        action="store_true",
        default=False,
        help="Store sync results into the database",
    )

    return parser.parse_args()


def main() -> int:
    color_logger.configure_logging()
    args = get_args()
    run_test(args=args)

    return 0


if __name__ == "__main__":
    sys.exit(main())
