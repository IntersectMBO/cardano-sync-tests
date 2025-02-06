import argparse
import datetime
import json
import logging
import os
import pathlib as pl
import re
import shutil
import sys
import typing as tp

from sync_tests.utils import helpers
from sync_tests.utils import node

LOGGER = logging.getLogger(__name__)

CONFIGS_BASE_URL = "https://book.play.dev.cardano.org/environments"
NODE = pl.Path.cwd() / "cardano-node"
CLI = pl.Path.cwd() / "cardano-cli"
NODE_LOG_FILE_NAME = "logfile.log"
NODE_LOG_FILE_ARTIFACT = "node.log"
RESULTS_FILE_NAME = "sync_results.json"


def get_data_from_logs(log_file: pl.Path) -> dict:
    tip_details_dict = {}
    heap_ram_details_dict = {}
    rss_ram_details_dict = {}
    centi_cpu_dict = {}
    cpu_details_dict: dict[float, tp.Any] = {}
    logs_details_dict = {}

    with open(log_file) as f:
        log_file_lines = [line.rstrip() for line in f]

    for line in log_file_lines:
        if "cardano.node.resources" in line:
            timestamp = re.findall(r"\d{4}-\d{2}-\d{2} \d{1,2}:\d{1,2}:\d{1,2}", line)[0]
            heap_ram_value = re.findall(
                r'"Heap",Number [-+]?[\d]+\.?[\d]*[Ee](?:[-+]?[\d]+)?', line
            )
            rss_ram_value = re.findall(r'"RSS",Number [-+]?[\d]+\.?[\d]*[Ee](?:[-+]?[\d]+)?', line)
            if len(heap_ram_value) > 0:
                heap_ram_details_dict[timestamp] = heap_ram_value[0].split(" ")[1]
            if len(rss_ram_value) > 0:
                rss_ram_details_dict[timestamp] = rss_ram_value[0].split(" ")[1]

            centi_cpu = re.findall(r'"CentiCpu",Number \d+\.\d+', line)
            if len(centi_cpu) > 0:
                centi_cpu_dict[timestamp] = centi_cpu[0].split(" ")[1]
        if "new tip" in line:
            timestamp = re.findall(r"\d{4}-\d{2}-\d{2} \d{1,2}:\d{1,2}:\d{1,2}", line)[0]
            slot_no = line.split(" at slot ")[1]
            tip_details_dict[timestamp] = slot_no

    no_of_cpu_cores = os.cpu_count() or 1
    timestamps_list = list(centi_cpu_dict.keys())
    for timestamp1 in timestamps_list[1:]:
        # %CPU = dValue / dt for 1 core
        previous_timestamp = datetime.datetime.strptime(
            timestamps_list[timestamps_list.index(timestamp1) - 1], "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=datetime.timezone.utc)
        current_timestamp = datetime.datetime.strptime(
            timestamps_list[timestamps_list.index(timestamp1)], "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=datetime.timezone.utc)
        previous_value = float(
            centi_cpu_dict[timestamps_list[timestamps_list.index(timestamp1) - 1]]
        )
        current_value = float(centi_cpu_dict[timestamps_list[timestamps_list.index(timestamp1)]])
        cpu_load_percent = (current_value - previous_value) / (
            current_timestamp - previous_timestamp
        ).total_seconds()

        cpu_details_dict[timestamp1] = cpu_load_percent / no_of_cpu_cores

    all_timestamps_list = set(
        list(tip_details_dict.keys())
        + list(heap_ram_details_dict.keys())
        + list(rss_ram_details_dict.keys())
        + list(cpu_details_dict.keys())
    )
    for timestamp2 in all_timestamps_list:
        if timestamp2 not in list(tip_details_dict.keys()):
            tip_details_dict[timestamp2] = ""
        if timestamp2 not in list(heap_ram_details_dict.keys()):
            heap_ram_details_dict[timestamp2] = ""
        if timestamp2 not in list(rss_ram_details_dict.keys()):
            rss_ram_details_dict[timestamp2] = ""
        if timestamp2 not in list(cpu_details_dict.keys()):
            cpu_details_dict[timestamp2] = ""

        logs_details_dict[timestamp2] = {
            "tip": tip_details_dict[timestamp2],
            "heap_ram": heap_ram_details_dict[timestamp2],
            "rss_ram": rss_ram_details_dict[timestamp2],
            "cpu": cpu_details_dict[timestamp2],
        }

    return logs_details_dict


def run_sync_test(args: argparse.Namespace) -> None:
    repository = None
    secs_to_start1, secs_to_start2 = 0, 0

    conf_dir = pl.Path.cwd()
    base_dir = pl.Path.cwd()

    node.set_node_socket_path_env_var(base_dir=base_dir)

    print("--- Test data information", flush=True)
    start_test_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
    helpers.print_message(type="info", message=f"Test start time: {start_test_time}")
    helpers.print_message(type="warn", message="Test parameters:")
    env = helpers.get_arg_value(args=args, key="environment")
    node_build_mode = helpers.get_arg_value(args=args, key="build_mode")
    node_rev1 = helpers.get_arg_value(args=args, key="node_rev1")
    node_rev2 = helpers.get_arg_value(args=args, key="node_rev2")
    tag_no1 = helpers.get_arg_value(args=args, key="tag_no1")
    tag_no2 = helpers.get_arg_value(args=args, key="tag_no2")
    node_topology_type1 = helpers.get_arg_value(args=args, key="node_topology1")
    node_topology_type2 = helpers.get_arg_value(args=args, key="node_topology2")
    node_start_arguments1 = helpers.get_arg_value(args=args, key="node_start_arguments1")
    node_start_arguments2 = helpers.get_arg_value(args=args, key="node_start_arguments2")
    use_genesis_mode = helpers.get_arg_value(args=args, key="use_genesis_mode")

    print(f"- env: {env}")
    print(f"- node_build_mode: {node_build_mode}")
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
    helpers.print_message(
        type="info",
        message=f"Get the cardano-node and cardano-cli files using - {node_build_mode}",
    )
    start_build_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
    if "windows" not in platform_system.lower():
        repository = node.get_node_files(node_rev1)
    elif "windows" in platform_system.lower():
        repository = node.get_node_files(node_rev1, build_tool="cabal")
    else:
        helpers.print_message(
            type="error",
            message=(
                f"ERROR: method not implemented yet!!! "
                f"Only building with NIX is supported at this moment - {node_build_mode}"
            ),
        )
        sys.exit(1)
    end_build_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
    helpers.print_message(type="info", message=f"  - start_build_time: {start_build_time}")
    helpers.print_message(type="info", message=f"  - end_build_time: {end_build_time}")

    helpers.print_message(type="warn", message="--- node version ")
    cli_version1, cli_git_rev1 = node.get_node_version()
    print(f"  - cardano_cli_version1: {cli_version1}")
    print(f"  - cardano_cli_git_rev1: {cli_git_rev1}")

    print("--- Get the node configuration files")
    node.rm_node_config_files(conf_dir=conf_dir)
    # TO DO: change the default to P2P when full P2P will be supported on Mainnet
    node.get_node_config_files(
        env=env,
        conf_dir=conf_dir,
        node_topology_type=node_topology_type1,
        use_genesis_mode=use_genesis_mode,
    )

    print("Configure node")
    node.configure_node(config_file=conf_dir / "config.json")
    if env == "mainnet" and node_topology_type1 == "legacy":
        node.disable_p2p_node_config(config_file=conf_dir / "config.json")

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
    start_sync_time1 = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
    if "None" in node_start_arguments1:
        node_start_arguments1 = []

    node_proc1, logfile1 = node.start_node(
        cardano_node=NODE, base_dir=base_dir, node_start_arguments=node_start_arguments1
    )
    secs_to_start1 = node.wait_node_start(env=env, timeout_minutes=10)

    helpers.print_message(type="info", message=" - waiting for the node to sync")

    sync1_error = False
    try:
        (
            sync_time_seconds1,
            last_slot_no1,
            latest_chunk_no1,
            era_details_dict1,
            epoch_details_dict1,
        ) = node.wait_for_node_to_sync(env=env, base_dir=base_dir)
    except Exception as e:
        sync1_error = True
        helpers.print_message(
            type="error",
            message=f" !!! ERROR - could not finish sync1 - {e}",
        )
    finally:
        end_sync_time1 = datetime.datetime.now(tz=datetime.timezone.utc).strftime(
            "%d/%m/%Y %H:%M:%S"
        )
        helpers.print_message(type="warn", message=f"Stop node for: {node_rev1}")
        exit_code1 = node.stop_node(proc=node_proc1)
        helpers.print_message(type="warn", message=f"Exit code: {exit_code1}")
        logfile1.flush()
        logfile1.close()

    if sync1_error:
        sys.exit(1)

    print(f"secs_to_start1: {secs_to_start1}")
    print(f"start_sync_time1: {start_sync_time1}")
    print(f"end_sync_time1: {end_sync_time1}")

    # we are interested in the node logs only for the main sync - using tag_no1
    test_values_dict: dict[str, tp.Any] = {}
    print("--- Parse the node logs and get the relevant data")
    logs_details_dict = get_data_from_logs(log_file=base_dir / NODE_LOG_FILE_NAME)
    test_values_dict["log_values"] = json.dumps(logs_details_dict)

    print(f"--- Start node using tag_no2: {tag_no2}")
    (
        _cardano_cli_version2,
        _cardano_cli_git_rev2,
        _shelley_sync_time_seconds2,
        _total_chunks2,
        _latest_block_no2,
        _latest_slot_no2,
        start_sync_time2,
        end_sync_time2,
        _start_sync_time3,
        _sync_time_after_restart_seconds,
        cli_version2,
        cli_git_rev2,
        last_slot_no2,
        latest_chunk_no2,
        sync_time_seconds2,
    ) = (
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        0,
    )
    if tag_no2 and tag_no2 != "None":
        node.delete_node_files()
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
        if "windows" not in platform_system.lower():
            node.get_node_files(node_rev2, repository)
        elif "windows" in platform_system.lower():
            node.get_node_files(node_rev2, repository, build_tool="cabal")
        else:
            helpers.print_message(
                type="error",
                message=(
                    "ERROR: method not implemented yet!!! "
                    f"Only building with NIX is supported at this moment - {node_build_mode}"
                ),
            )
            sys.exit(1)

        if env == "mainnet" and (node_topology_type1 != node_topology_type2):
            helpers.print_message(type="warn", message="remove the previous topology")
            (conf_dir / "topology.json").unlink()
            print("Getting the node configuration files")
            node.get_node_config_files(
                env=env, conf_dir=conf_dir, node_topology_type=node_topology_type2,
            )

        helpers.print_message(type="warn", message="node version")
        cli_version2, cli_git_rev2 = node.get_node_version()
        print(f" - cardano_cli_version2: {cli_version2}")
        print(f" - cardano_cli_git_rev2: {cli_git_rev2}")
        print()
        print(f"================ Start node using node_rev2: {node_rev2} ====================")
        start_sync_time2 = datetime.datetime.now(tz=datetime.timezone.utc).strftime(
            "%d/%m/%Y %H:%M:%S"
        )
        if "None" in node_start_arguments1:
            node_start_arguments2 = []
        node_proc2, logfile2 = node.start_node(
            cardano_node=NODE, base_dir=base_dir, node_start_arguments=node_start_arguments2
        )
        secs_to_start2 = node.wait_node_start(env=env)

        helpers.print_message(
            type="info",
            message=f" - waiting for the node to sync - using node_rev2: {node_rev2}",
        )

        sync2_error = False
        try:
            (
                sync_time_seconds2,
                last_slot_no2,
                latest_chunk_no2,
                era_details_dict2,
                epoch_details_dict2,
            ) = node.wait_for_node_to_sync(env=env, base_dir=base_dir)
        except Exception as e:
            sync2_error = True
            helpers.print_message(
                type="error",
                message=f" !!! ERROR - could not finish sync2 - {e}",
            )
        finally:
            end_sync_time2 = datetime.datetime.now(tz=datetime.timezone.utc).strftime(
                "%d/%m/%Y %H:%M:%S"
            )
            helpers.print_message(type="warn", message=f"Stop node for: {node_rev2}")
            exit_code2 = node.stop_node(proc=node_proc2)
            helpers.print_message(type="warn", message=f"Exit code: {exit_code2}")
            logfile2.flush()
            logfile2.close()

        if sync2_error:
            sys.exit(1)

    chain_size = helpers.get_directory_size(base_dir / "db")

    print("--- Node sync test completed")
    print("Node sync test ended; Creating the `test_values_dict` dict with the test values")
    print("++++++++++++++++++++++++++++++++++++++++++++++")
    for era in era_details_dict1:
        print(f"  *** {era} --> {era_details_dict1[era]}")
        test_values_dict[str(era + "_start_time")] = era_details_dict1[era]["start_time"]
        test_values_dict[str(era + "_start_epoch")] = era_details_dict1[era]["start_epoch"]
        test_values_dict[str(era + "_slots_in_era")] = era_details_dict1[era]["slots_in_era"]
        test_values_dict[str(era + "_start_sync_time")] = era_details_dict1[era]["start_sync_time"]
        test_values_dict[str(era + "_end_sync_time")] = era_details_dict1[era]["end_sync_time"]
        test_values_dict[str(era + "_sync_duration_secs")] = era_details_dict1[era][
            "sync_duration_secs"
        ]
        test_values_dict[str(era + "_sync_speed_sps")] = era_details_dict1[era]["sync_speed_sps"]
    print("++++++++++++++++++++++++++++++++++++++++++++++")
    epoch_details = {}
    for epoch in epoch_details_dict1:
        print(f"{epoch} --> {epoch_details_dict1[epoch]}")
        epoch_details[epoch] = epoch_details_dict1[epoch]["sync_duration_secs"]
    print("++++++++++++++++++++++++++++++++++++++++++++++")
    test_values_dict["env"] = env
    test_values_dict["tag_no1"] = tag_no1
    test_values_dict["tag_no2"] = tag_no2
    test_values_dict["cli_version1"] = cli_version1
    test_values_dict["cli_version2"] = cli_version2
    test_values_dict["cli_git_rev1"] = cli_git_rev1
    test_values_dict["cli_git_rev2"] = cli_git_rev2
    test_values_dict["start_sync_time1"] = start_sync_time1
    test_values_dict["end_sync_time1"] = end_sync_time1
    test_values_dict["start_sync_time2"] = start_sync_time2
    test_values_dict["end_sync_time2"] = end_sync_time2
    test_values_dict["last_slot_no1"] = last_slot_no1
    test_values_dict["last_slot_no2"] = last_slot_no2
    test_values_dict["start_node_secs1"] = secs_to_start1
    test_values_dict["start_node_secs2"] = secs_to_start2
    test_values_dict["sync_time_seconds1"] = sync_time_seconds1
    test_values_dict["sync_time1"] = str(datetime.timedelta(seconds=int(sync_time_seconds1)))
    test_values_dict["sync_time_seconds2"] = sync_time_seconds2
    test_values_dict["sync_time2"] = str(datetime.timedelta(seconds=int(sync_time_seconds2)))
    test_values_dict["total_chunks1"] = latest_chunk_no1
    test_values_dict["total_chunks2"] = latest_chunk_no2
    test_values_dict["platform_system"] = platform_system
    test_values_dict["platform_release"] = platform_release
    test_values_dict["platform_version"] = platform_version
    test_values_dict["chain_size_bytes"] = chain_size
    test_values_dict["sync_duration_per_epoch"] = json.dumps(epoch_details)
    test_values_dict["eras_in_test"] = json.dumps(list(era_details_dict1.keys()))
    test_values_dict["no_of_cpu_cores"] = os.cpu_count()
    test_values_dict["total_ram_in_GB"] = helpers.get_total_ram_in_gb()
    test_values_dict["epoch_no_d_zero"] = node.get_epoch_no_d_zero(env=env)
    test_values_dict["start_slot_no_d_zero"] = node.get_start_slot_no_d_zero(env=env)
    test_values_dict["hydra_eval_no1"] = node_rev1
    test_values_dict["hydra_eval_no2"] = node_rev2

    print("--- Write tests results to file")
    current_directory = pl.Path.cwd()
    print(f"current_directory: {current_directory}")
    print(f"Write the test values to the {current_directory / RESULTS_FILE_NAME} file")
    with open(RESULTS_FILE_NAME, "w") as results_file:
        json.dump(test_values_dict, results_file, indent=2)

    print("--- Copy the node logs")
    # sometimes uploading the artifacts fails because the node still writes into
    # the log file during the upload even though an attepmt to stop it was made
    shutil.copy(base_dir / NODE_LOG_FILE_NAME, base_dir / NODE_LOG_FILE_ARTIFACT)


def get_args() -> argparse.Namespace:
    """Get command line arguments."""
    parser = argparse.ArgumentParser(description="Run Cardano Node sync test\n\n")

    parser.add_argument(
        "-b", "--build_mode", help="how to get the node files - nix, cabal, prebuilt"
    )
    parser.add_argument(
        "-e",
        "--environment",
        help="the environment on which to run the sync test - preview, preprod, mainnet",
    )
    parser.add_argument(
        "-r1",
        "--node_rev1",
        help=(
            "desired cardano-node revision - cardano-node tag or branch "
            "(used for initial sync, from clean state)"
        ),
    )
    parser.add_argument(
        "-r2",
        "--node_rev2",
        help=(
            "desired cardano-node revision - cardano-node tag or branch "
            "(used for final sync, from existing state)"
        ),
    )
    parser.add_argument(
        "-t1",
        "--tag_no1",
        help="tag_no1 label as it will be shown in the db/visuals",
    )
    parser.add_argument(
        "-t2",
        "--tag_no2",
        help="tag_no2 label as it will be shown in the db/visuals",
    )
    parser.add_argument(
        "-n1",
        "--node_topology1",
        help=(
            "type of node topology used for the initial sync - "
            "legacy, non-bootstrap-peers, bootstrap-peers"
        ),
    )
    parser.add_argument(
        "-n2",
        "--node_topology2",
        help=(
            "type of node topology used for final sync (after restart) - "
            "legacy, non-bootstrap-peers, bootstrap-peers"
        ),
    )
    parser.add_argument(
        "-a1",
        "--node_start_arguments1",
        nargs="+",
        type=str,
        help="arguments to be passed when starting the node from clean state (first tag_no)",
    )
    parser.add_argument(
        "-a2",
        "--node_start_arguments2",
        nargs="+",
        type=str,
        help="arguments to be passed when starting the node from existing state (second tag_no)",
    )
    parser.add_argument(
        "-g",
        "--use_genesis_mode",
        type=lambda x: x.lower() == "true",
        default=False,
        help="use_genesis_mode",
    )

    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        format="%(name)s:%(levelname)s:%(message)s",
        level=logging.INFO,
    )
    args = get_args()
    run_sync_test(args=args)

    return 0


if __name__ == "__main__":
    sys.exit(main())
