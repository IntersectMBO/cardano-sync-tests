import argparse
import datetime
import heapq
import itertools
import json
import logging
import os
import pathlib as pl
import re
import shutil
import subprocess
import sys
import typing as tp

import colorama

from sync_tests.utils import helpers
from sync_tests.utils import node

LOGGER = logging.getLogger(__name__)

NODE_LOG_FILE_ARTIFACT = "node.log"
RESULTS_FILE_NAME = "sync_results.json"


class ColorFormatter(logging.Formatter):
    COLORS: tp.ClassVar[dict[str, str]] = {
        "WARNING": colorama.Fore.YELLOW,
        "ERROR": colorama.Fore.RED,
        "DEBUG": colorama.Fore.BLUE,
        # Keep "INFO" uncolored
        "CRITICAL": colorama.Fore.RED,
    }

    def format(self, record: logging.LogRecord) -> str:
        color: str | None = self.COLORS.get(record.levelname)
        if color:
            record.name = f"{color}{record.name}{colorama.Style.RESET_ALL}"
            record.levelname = f"{color}{record.levelname}{colorama.Style.RESET_ALL}"
            record.msg = f"{color}{record.msg}{colorama.Style.RESET_ALL}"
        return super().format(record)


class ColorLogger(logging.Logger):
    def __init__(self, name: str) -> None:
        super().__init__(name, logging.INFO)
        color_formatter = ColorFormatter("%(message)s")
        console = logging.StreamHandler()
        console.setFormatter(color_formatter)
        self.addHandler(console)


def merge_sorted_unique(*iterables: tp.Iterable) -> list:
    """Merge and sort multiple sorted iterables while removing duplicates."""
    return [key for key, _ in itertools.groupby(heapq.merge(*iterables))]


def filtered_log_fd(log_file: pl.Path, use_rg: bool = False) -> subprocess.Popen:
    """Run rg or grep to filter log lines and return a file descriptor for reading."""
    if use_rg:
        cmd = ["rg", "cardano\\.node\\.resources|new tip", str(log_file)]
    else:
        cmd = ["grep", "-E", "cardano\\.node\\.resources|new tip", str(log_file)]

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,  # Suppress errors (e.g., if file is missing)
        text=True,  # Ensures output is treated as text, not bytes
    )

    return process


def get_data_from_logs(log_file: pl.Path) -> dict[str, dict]:
    """Extract relevant data from the log file and return a dictionary."""
    chunk_size = 512 * 1024  # 512 KB
    tip_details_dict: dict[datetime.datetime, int] = {}
    heap_ram_details_dict: dict[datetime.datetime, float] = {}
    rss_ram_details_dict: dict[datetime.datetime, float] = {}
    centi_cpu_dict: dict[datetime.datetime, float] = {}
    cpu_details_dict: dict[datetime.datetime, float] = {}
    logs_details_dict: dict[str, dict[str, tp.Any]] = {}

    timestamp_pattern = re.compile(r"\d{4}-\d{2}-\d{2} \d{1,2}:\d{1,2}:\d{1,2}")
    heap_pattern = re.compile(r'"Heap",Number ([-+]?\d+\.?\d*(?:[Ee][-+]?\d+)?)')
    rss_pattern = re.compile(r'"RSS",Number ([-+]?\d+\.?\d*(?:[Ee][-+]?\d+)?)')
    centi_cpu_pattern = re.compile(r'"CentiCpu",Number (\d+\.\d+)')

    def _process_log_line(line: str) -> None:
        """Extract relevant data from a log line and updates dictionaries."""
        # Extract numeric values for heap, RSS, and CPU if they exist
        if (
            "cardano.node.resources" in line
            and (timestamp_match := timestamp_pattern.search(line))
            and (heap_match := heap_pattern.search(line))
            and (rss_match := rss_pattern.search(line))
            and (centi_cpu_match := centi_cpu_pattern.search(line))
        ):
            timestamp = datetime.datetime.strptime(
                timestamp_match.group(0), "%Y-%m-%d %H:%M:%S"
            ).replace(tzinfo=datetime.timezone.utc)
            heap_ram_details_dict[timestamp] = float(heap_match.group(1))
            rss_ram_details_dict[timestamp] = float(rss_match.group(1))
            centi_cpu_dict[timestamp] = float(centi_cpu_match.group(1))

        # Extract slot number
        elif (
            "new tip" in line
            and " at slot " in line
            and (timestamp_match := timestamp_pattern.search(line))
        ):
            timestamp = datetime.datetime.strptime(
                timestamp_match.group(0), "%Y-%m-%d %H:%M:%S"
            ).replace(tzinfo=datetime.timezone.utc)
            tip_details_dict[timestamp] = int(line.split(" at slot ", 1)[1])

    def _process_log_file(infile: tp.IO) -> None:
        # Read the file in chunks to handle large logs efficiently
        incomplete_line = ""
        while chunk := infile.read(chunk_size):
            if incomplete_line:
                chunk = incomplete_line + chunk  # Prepend leftover from previous chunk

            lines = chunk.splitlines(keepends=False)

            # Handle incomplete lines at the end of the chunk
            incomplete_line = lines.pop() if chunk[-1] not in "\n\r" else ""

            # Process each complete log line
            for line in lines:
                _process_log_line(line)

        # Process any remaining incomplete line
        if incomplete_line:
            _process_log_line(incomplete_line)

    # Filter the log file with 'rg' (ripgrep) if available
    if shutil.which("rg"):
        with filtered_log_fd(log_file, use_rg=True) as process:
            if process.stdout:
                _process_log_file(infile=process.stdout)
    # If 'rg' is not available, check if 'grep' is available
    if not centi_cpu_dict and shutil.which("grep"):
        with filtered_log_fd(log_file) as process:
            if process.stdout:
                _process_log_file(infile=process.stdout)
    # If neither 'rg' nor 'grep' is available, read the log file directly without filtering
    if not centi_cpu_dict:
        with open(log_file, encoding="utf-8") as infile:
            _process_log_file(infile=infile)

    # Compute CPU load percentage per core
    no_of_cpu_cores = os.cpu_count() or 1
    timestamps_list = list(centi_cpu_dict)

    for prev_timestamp, curr_timestamp in zip(timestamps_list, timestamps_list[1:]):
        prev_value = centi_cpu_dict[prev_timestamp]
        curr_value = centi_cpu_dict[curr_timestamp]

        # Compute CPU load percentage over elapsed time
        cpu_load_percent = (curr_value - prev_value) / (
            curr_timestamp - prev_timestamp
        ).total_seconds()
        cpu_details_dict[curr_timestamp] = cpu_load_percent / no_of_cpu_cores

    # Collect all unique timestamps from different dictionaries
    all_timestamps_list = merge_sorted_unique(tip_details_dict, cpu_details_dict)

    # Populate logs_details_dict with merged data
    tip = -1
    for timestamp in all_timestamps_list:
        # Use the last known tip if no new tip is available
        tip = tip_details_dict.get(timestamp, tip)
        if tip == -1:
            continue
        cpu = cpu_details_dict.get(timestamp)
        if cpu is None:
            continue
        logs_details_dict[str(timestamp)] = {
            "tip": tip,
            "heap_ram": heap_ram_details_dict.get(timestamp, 0.0),
            "rss_ram": rss_ram_details_dict.get(timestamp, 0.0),
            "cpu": cpu,
        }

    return logs_details_dict


def run_test(args: argparse.Namespace) -> None:
    conf_dir = pl.Path.cwd()
    base_dir = pl.Path.cwd()

    node.set_node_socket_path_env_var(base_dir=base_dir)

    print("--- Test data information", flush=True)
    start_test_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
    helpers.print_message(type="info", message=f"Test start time: {start_test_time}")
    helpers.print_message(type="warn", message="Test parameters:")
    env = helpers.get_arg_value(args=args, key="environment")
    node_build_mode = helpers.get_arg_value(args=args, key="build_mode") or "nix"
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
    node.config_sync(
        env=env,
        conf_dir=conf_dir,
        node_build_mode=node_build_mode,
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
    sync1_rec = node.run_sync(
        node_start_arguments=node_start_arguments1, base_dir=base_dir, env=env
    )
    if not sync1_rec:
        sys.exit(1)

    print(f"secs_to_start1: {sync1_rec.secs_to_start}")
    print(f"start_sync_time1: {sync1_rec.start_sync_time}")
    print(f"end_sync_time1: {sync1_rec.end_sync_time}")

    # we are interested in the node logs only for the main sync - using tag_no1
    test_values_dict: dict[str, tp.Any] = {}
    print("--- Parse the node logs and get the relevant data")
    logs_details_dict = get_data_from_logs(log_file=base_dir / node.NODE_LOG_FILE_NAME)
    test_values_dict["log_values"] = logs_details_dict

    sync2_rec = None
    print(f"--- Start node using tag_no2: {tag_no2}")
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
        node.config_sync(
            env=env,
            conf_dir=conf_dir,
            node_build_mode=node_build_mode,
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
            node_start_arguments=node_start_arguments2, base_dir=base_dir, env=env
        )
        if not sync2_rec:
            sys.exit(1)

    chain_size = helpers.get_directory_size(base_dir / "db")

    print("--- Node sync test completed")
    print("Node sync test ended; Creating the `test_values_dict` dict with the test values")
    print("++++++++++++++++++++++++++++++++++++++++++++++")
    for era, era_data in sync1_rec.era_details.items():
        print(f"  *** {era} --> {era_data}")
        test_values_dict[f"{era}_start_time"] = era_data["start_time"]
        test_values_dict[f"{era}_start_epoch"] = era_data["start_epoch"]
        test_values_dict[f"{era}_slots_in_era"] = era_data["slots_in_era"]
        test_values_dict[f"{era}_start_sync_time"] = era_data["start_sync_time"]
        test_values_dict[f"{era}_end_sync_time"] = era_data["end_sync_time"]
        test_values_dict[f"{era}_sync_duration_secs"] = era_data["sync_duration_secs"]
        test_values_dict[f"{era}_sync_speed_sps"] = era_data["sync_speed_sps"]
    print("++++++++++++++++++++++++++++++++++++++++++++++")

    epoch_details = {}
    for epoch, epoch_data in sync1_rec.epoch_details.items():
        epoch_details[epoch] = epoch_data["sync_duration_secs"]
    print("++++++++++++++++++++++++++++++++++++++++++++++")

    test_values_dict["env"] = env
    test_values_dict["tag_no1"] = tag_no1
    test_values_dict["tag_no2"] = tag_no2
    test_values_dict["cli_version1"] = cli_version1
    test_values_dict["cli_version2"] = cli_version2 if sync2_rec else None
    test_values_dict["cli_git_rev1"] = cli_git_rev1
    test_values_dict["cli_git_rev2"] = cli_git_rev2 if sync2_rec else None
    test_values_dict["start_sync_time1"] = sync1_rec.start_sync_time
    test_values_dict["end_sync_time1"] = sync1_rec.end_sync_time
    test_values_dict["start_sync_time2"] = sync2_rec.start_sync_time if sync2_rec else None
    test_values_dict["end_sync_time2"] = sync2_rec.end_sync_time if sync2_rec else None
    test_values_dict["last_slot_no1"] = sync1_rec.last_slot_no
    test_values_dict["last_slot_no2"] = sync2_rec.last_slot_no if sync2_rec else None
    test_values_dict["start_node_secs1"] = sync1_rec.secs_to_start
    test_values_dict["start_node_secs2"] = sync2_rec.secs_to_start if sync2_rec else None
    test_values_dict["sync_time_seconds1"] = sync1_rec.sync_time_sec
    test_values_dict["sync_time1"] = str(datetime.timedelta(seconds=sync1_rec.sync_time_sec))
    test_values_dict["sync_time_seconds2"] = sync2_rec.sync_time_sec if sync2_rec else None
    test_values_dict["sync_time2"] = (
        str(datetime.timedelta(seconds=int(sync2_rec.sync_time_sec))) if sync2_rec else None
    )
    test_values_dict["total_chunks1"] = sync1_rec.latest_chunk_no
    test_values_dict["total_chunks2"] = sync2_rec.latest_chunk_no if sync2_rec else None
    test_values_dict["platform_system"] = platform_system
    test_values_dict["platform_release"] = platform_release
    test_values_dict["platform_version"] = platform_version
    test_values_dict["chain_size_bytes"] = chain_size
    test_values_dict["sync_duration_per_epoch"] = epoch_details
    test_values_dict["eras_in_test"] = list(sync1_rec.era_details.keys())
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
    shutil.copy(base_dir / node.NODE_LOG_FILE_NAME, base_dir / NODE_LOG_FILE_ARTIFACT)


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
    logging.setLoggerClass(ColorLogger)
    args = get_args()
    run_test(args=args)

    return 0


if __name__ == "__main__":
    sys.exit(main())
