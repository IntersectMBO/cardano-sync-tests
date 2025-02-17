import argparse
import contextlib
import dataclasses
import datetime
import fileinput
import heapq
import itertools
import json
import logging
import os
import pathlib as pl
import platform
import re
import shutil
import stat
import subprocess
import sys
import time
import typing as tp
import urllib.request

import colorama
import git

from sync_tests.utils import blockfrost
from sync_tests.utils import cli
from sync_tests.utils import exceptions
from sync_tests.utils import explorer
from sync_tests.utils import gitpython
from sync_tests.utils import helpers

LOGGER = logging.getLogger(__name__)

CONFIGS_BASE_URL = "https://book.play.dev.cardano.org/environments"
NODE = pl.Path.cwd() / "cardano-node"
CLI = str(pl.Path.cwd() / "cardano-cli")
NODE_LOG_FILE_NAME = "logfile.log"
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


@dataclasses.dataclass(frozen=True)
class SyncRec:
    secs_to_start: int
    sync_time_sec: int
    last_slot_no: int
    latest_chunk_no: int
    era_details: dict
    epoch_details: dict
    start_sync_time: str
    end_sync_time: str


@contextlib.contextmanager
def temporary_chdir(path: pl.Path) -> tp.Iterator[None]:
    prev_cwd = pl.Path.cwd()  # Store the current working directory
    try:
        os.chdir(path)  # Change to the new directory
        yield
    finally:
        os.chdir(prev_cwd)  # Restore the original working directory


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


def delete_node_files() -> None:
    for p in pl.Path("..").glob("cardano-*"):
        if p.is_dir():
            LOGGER.info(f"deleting directory: {p}")
            shutil.rmtree(p)  # Use shutil.rmtree to delete directories
        else:
            LOGGER.info(f"deleting file: {p}")
            p.unlink(missing_ok=True)


def make_executable(path: pl.Path) -> None:
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def rm_node_config_files(conf_dir: pl.Path) -> None:
    LOGGER.info("Removing existing config files")
    for gen in conf_dir.glob("*-genesis.json"):
        pl.Path(gen).unlink(missing_ok=True)
    for f in ("config.json", "topology.json"):
        (conf_dir / f).unlink(missing_ok=True)


def download_config_file(config_slug: str, save_as: pl.Path) -> None:
    url = f"{CONFIGS_BASE_URL}/{config_slug}"
    LOGGER.info(f"Downloading '{url}' and saving as '{save_as}'")
    urllib.request.urlretrieve(url, save_as)


def get_node_config_files(
    env: str, node_topology_type: str, conf_dir: pl.Path, use_genesis_mode: bool = False
) -> None:
    LOGGER.info("Getting the node configuration files")
    config_file_path = conf_dir / "config.json"
    topology_file_path = conf_dir / "topology.json"

    download_config_file(config_slug=f"{env}/config.json", save_as=config_file_path)
    download_config_file(
        config_slug=f"{env}/byron-genesis.json", save_as=conf_dir / "byron-genesis.json"
    )
    download_config_file(
        config_slug=f"{env}/shelley-genesis.json", save_as=conf_dir / "shelley-genesis.json"
    )
    download_config_file(
        config_slug=f"{env}/alonzo-genesis.json", save_as=conf_dir / "alonzo-genesis.json"
    )
    download_config_file(
        config_slug=f"{env}/conway-genesis.json", save_as=conf_dir / "conway-genesis.json"
    )

    if env == "mainnet" and node_topology_type == "non-bootstrap-peers":
        download_config_file(
            config_slug=f"{env}/topology-non-bootstrap-peers.json",
            save_as=topology_file_path,
        )
    elif env == "mainnet" and node_topology_type == "legacy":
        download_config_file(config_slug=f"{env}/topology-legacy.json", save_as=topology_file_path)
    else:
        download_config_file(config_slug=f"{env}/topology.json", save_as=topology_file_path)

    if use_genesis_mode:
        enable_genesis_mode(config_file=config_file_path, topology_file=topology_file_path)


def configure_node(config_file: pl.Path) -> None:
    LOGGER.info("Configuring node")
    with open(config_file) as json_file:
        node_config_json = json.load(json_file)

    # Use the legacy tracing system
    node_config_json["TraceOptions"] = {}
    node_config_json["UseTraceDispatcher"] = False

    # Set min severity
    node_config_json["minSeverity"] = "Info"

    # Enable resource monitoring
    node_config_json["options"]["mapBackends"]["cardano.node.resources"] = ["KatipBK"]

    with open(config_file, "w") as json_file:
        json.dump(node_config_json, json_file, indent=2)


def disable_p2p_node_config(config_file: pl.Path) -> None:
    """Disable P2P settings in the node configuration file."""
    helpers.update_json_file(
        file_path=config_file, updates={"EnableP2P": False, "PeerSharing": False}
    )


def enable_genesis_mode(config_file: pl.Path, topology_file: pl.Path) -> None:
    """Enable Genesis mode in the node configuration and topology files."""
    helpers.update_json_file(file_path=config_file, updates={"ConsensusMode": "GenesisMode"})
    helpers.update_json_file(
        file_path=topology_file,
        updates={"peerSnapshotFile": "sync_tests/data/peersnapshotfile.json"},
    )


def set_node_socket_path_env_var(base_dir: pl.Path) -> None:
    socket_path: str | pl.Path
    if "windows" in platform.system().lower():
        socket_path = "\\\\.\\pipe\\cardano-node"
    else:
        start_socket_path = os.environ.get("CARDANO_NODE_SOCKET_PATH")
        if start_socket_path is None:
            socket_path = (base_dir / "db" / "node.socket").expanduser().absolute()
        else:
            socket_path = pl.Path(start_socket_path)
    os.environ["CARDANO_NODE_SOCKET_PATH"] = str(socket_path)


def get_epoch_no_d_zero(env: str) -> int:
    if env == "mainnet":
        return 257
    return -1


def get_start_slot_no_d_zero(env: str) -> int:
    if env == "mainnet":
        return 25661009
    return -1


def get_testnet_args(env: str) -> list[str]:
    arg = []
    if env == "mainnet":
        arg = ["--mainnet"]
    if env == "preview":
        arg = ["--testnet-magic", "2"]
    if env == "preprod":
        arg = ["--testnet-magic", "1"]
    return arg


def get_current_tip(env: str) -> tuple:
    cmd = [CLI, "latest", "query", "tip", *get_testnet_args(env=env)]

    output = cli.cli(cli_args=cmd).stdout.decode("utf-8").strip()
    output_json = json.loads(output)
    epoch = int(output_json.get("epoch", 0))
    block = int(output_json.get("block", 0))
    hash_value = output_json.get("hash", "")
    slot = int(output_json.get("slot", 0))
    era = output_json.get("era", "").lower()
    sync_progress = (
        float(output_json.get("syncProgress", 0.0)) if "syncProgress" in output_json else None
    )

    return epoch, block, hash_value, slot, era, sync_progress


def wait_query_tip_available(env: str, timeout_minutes: int = 20) -> int:
    # when starting from clean state it might take ~30 secs for the cli to work
    # when starting from existing state it might take > 10 mins for the cli to work (opening db and
    # replaying the ledger)
    start_counter = time.perf_counter()

    str_err = ""
    for i in range(timeout_minutes):
        try:
            get_current_tip(env=env)
            break
        except exceptions.SyncError as e:
            str_err = str(e)
            if "Invalid argument" in str_err:
                raise
            now = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
            LOGGER.warning(f"{now} - Waiting 60s before retrying to get the tip again - {i}")
        time.sleep(60)
    else:
        err_raise = f"Failed to wait for tip: {str_err}"
        raise exceptions.SyncError(err_raise)

    stop_counter = time.perf_counter()
    start_time_seconds = int(stop_counter - start_counter)
    LOGGER.info(f"It took {start_time_seconds} seconds for the QUERY TIP command to be available")
    return start_time_seconds


def get_node_version() -> tuple[str, str]:
    try:
        cmd = f"{CLI} --version"
        output = (
            subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
            .decode("utf-8")
            .strip()
        )
        cardano_cli_version = output.split("git rev ")[0].strip()
        cardano_cli_git_rev = output.split("git rev ")[1].strip()
        return str(cardano_cli_version), str(cardano_cli_git_rev)
    except subprocess.CalledProcessError as e:
        msg = "command '{}' return with error (code {}): {}".format(
            e.cmd, e.returncode, " ".join(str(e.output).split())
        )
        raise RuntimeError(msg) from e


def start_node(
    cardano_node: pl.Path, base_dir: pl.Path, node_start_arguments: tp.Iterable[str]
) -> tuple[subprocess.Popen, tp.IO[str]]:
    start_args = " ".join(node_start_arguments)

    if platform.system().lower() == "windows":
        cmd = (
            f"{cardano_node} run --topology topology.json "
            f"--database-path {base_dir / 'db'} "
            "--host-addr 0.0.0.0 "
            "--port 3000 "
            "--socket-path \\\\.\\pipe\\cardano-node "
            f"--config config.json {start_args}"
        ).strip()
    else:
        socket_path = os.environ.get("CARDANO_NODE_SOCKET_PATH") or ""
        cmd = (
            f"{cardano_node} run --topology topology.json --database-path "
            f"{base_dir / 'db'} "
            "--host-addr 0.0.0.0 --port 3000 --config "
            f"config.json --socket-path {socket_path} {start_args}"
        ).strip()

    LOGGER.info(f"Starting node with cmd: {cmd}")
    logfile = open(base_dir / NODE_LOG_FILE_NAME, "w+")

    proc = subprocess.Popen(cmd.split(" "), stdout=logfile, stderr=logfile)
    return proc, logfile


def wait_node_start(env: str, timeout_minutes: int = 20) -> int:
    current_directory = pl.Path.cwd()

    LOGGER.info("Waiting for db folder to be created")
    count = 0
    count_timeout = 299
    while not pl.Path.is_dir(current_directory / "db"):
        time.sleep(1)
        count += 1
        if count > count_timeout:
            err_raise = f"Waited {count_timeout} seconds and the DB folder was not created yet"
            raise exceptions.SyncError(err_raise)

    LOGGER.info(f"DB folder was created after {count} seconds")
    secs_to_start = wait_query_tip_available(env=env, timeout_minutes=timeout_minutes)
    LOGGER.debug(f" - listdir current_directory: {os.listdir(current_directory)}")
    LOGGER.debug(f" - listdir db: {os.listdir(current_directory / 'db')}")
    return secs_to_start


def get_node_exit_code(proc: subprocess.Popen) -> int:
    """Get the exit code of a node process if it has finished."""
    if proc.poll() is None:  # None means the process is still running
        return -1

    # Get and report the exit code
    exit_code = proc.returncode
    return exit_code


def stop_node(proc: subprocess.Popen) -> int:
    if proc.poll() is None:  # None means the process is still running
        proc.terminate()
        try:
            proc.wait(timeout=10)  # Give it some time to exit gracefully
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

    # Get and report the exit code
    exit_code = proc.returncode
    return exit_code


def get_calculated_slot_no(env: str) -> int:
    current_time = datetime.datetime.now(tz=datetime.timezone.utc)
    shelley_start_time = byron_start_time = current_time

    if env == "mainnet":
        byron_start_time = datetime.datetime.strptime(
            "2017-09-23 21:44:51", "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=datetime.timezone.utc)
        shelley_start_time = datetime.datetime.strptime(
            "2020-07-29 21:44:51", "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=datetime.timezone.utc)
    elif env == "preprod":
        byron_start_time = datetime.datetime.strptime(
            "2022-06-01 00:00:00", "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=datetime.timezone.utc)
        shelley_start_time = datetime.datetime.strptime(
            "2022-06-21 00:00:00", "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=datetime.timezone.utc)
    elif env == "preview":
        # this env was started directly in Alonzo
        byron_start_time = datetime.datetime.strptime(
            "2022-08-09 00:00:00", "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=datetime.timezone.utc)
        shelley_start_time = datetime.datetime.strptime(
            "2022-08-09 00:00:00", "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=datetime.timezone.utc)

    last_slot_no = int(
        (shelley_start_time - byron_start_time).total_seconds() / 20
        + (current_time - shelley_start_time).total_seconds()
    )
    return last_slot_no


def wait_for_node_to_sync(env: str, base_dir: pl.Path) -> tuple:
    LOGGER.info("Waiting for the node to sync")
    era_details_dict = {}
    epoch_details_dict = {}

    actual_epoch, actual_block, actual_hash, actual_slot, actual_era, sync_progress = (
        get_current_tip(env=env)
    )
    last_slot_no = get_calculated_slot_no(env)
    start_sync = time.perf_counter()

    count = 0
    if sync_progress is not None:
        while sync_progress < 100:
            if count % 60 == 0:
                now = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
                LOGGER.warning(
                    f"{now} - actual_era  : {actual_era} "
                    f" - actual_epoch: {actual_epoch} "
                    f" - actual_block: {actual_block} "
                    f" - actual_slot : {actual_slot} "
                    f" - syncProgress: {sync_progress}",
                )
            if actual_era not in era_details_dict:
                current_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
                if env == "mainnet":
                    actual_era_start_time = blockfrost.get_epoch_start_datetime(actual_epoch)
                else:
                    actual_era_start_time = explorer.get_epoch_start_datetime_from_explorer(
                        env, actual_epoch
                    )
                actual_era_dict = {
                    "start_epoch": actual_epoch,
                    "start_time": actual_era_start_time,
                    "start_sync_time": current_time,
                }
                era_details_dict[actual_era] = actual_era_dict
            if actual_epoch not in epoch_details_dict:
                current_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
                actual_epoch_dict: dict[str, tp.Any] = {"start_sync_time": current_time}
                epoch_details_dict[actual_epoch] = actual_epoch_dict

            time.sleep(5)
            count += 1
            (
                actual_epoch,
                actual_block,
                actual_hash,
                actual_slot,
                actual_era,
                sync_progress,
            ) = get_current_tip(env=env)

    else:
        while actual_slot <= last_slot_no:
            if count % 60 == 0:
                now = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
                LOGGER.warning(
                    f"{now} - actual_era  : {actual_era} "
                    f" - actual_epoch: {actual_epoch} "
                    f" - actual_block: {actual_block} "
                    f" - actual_slot : {actual_slot} "
                    f" - syncProgress: {sync_progress}",
                )
            if actual_era not in era_details_dict:
                current_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
                if env == "mainnet":
                    actual_era_start_time = blockfrost.get_epoch_start_datetime(actual_epoch)
                else:
                    actual_era_start_time = explorer.get_epoch_start_datetime_from_explorer(
                        env, actual_epoch
                    )
                actual_era_dict = {
                    "start_epoch": actual_epoch,
                    "start_time": actual_era_start_time,
                    "start_sync_time": current_time,
                }
                era_details_dict[actual_era] = actual_era_dict
            if actual_epoch not in epoch_details_dict:
                current_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
                actual_epoch_dict = {"start_sync_time": current_time}
                epoch_details_dict[actual_epoch] = actual_epoch_dict
            time.sleep(1)
            count += 1
            (
                actual_epoch,
                actual_block,
                actual_hash,
                actual_slot,
                actual_era,
                sync_progress,
            ) = get_current_tip(env=env)

    end_sync = time.perf_counter()
    sync_time_seconds = int(end_sync - start_sync)
    LOGGER.info(f"sync_time_seconds: {sync_time_seconds}")

    chunk_files = sorted((base_dir / "db" / "immutable").iterdir(), key=lambda f: f.stat().st_mtime)
    latest_chunk_no = chunk_files[-1].stem
    LOGGER.info(f"Sync done!; latest_chunk_no: {latest_chunk_no}")

    # add "end_sync_time", "slots_in_era", "sync_duration_secs" and "sync_speed_sps" for each era;
    # for the last/current era, "end_sync_time" = current_utc_time / end_of_sync_time
    eras_list = list(era_details_dict.keys())
    for era in eras_list:
        if era == eras_list[-1]:
            end_sync_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            last_epoch = actual_epoch
        else:
            end_sync_time = era_details_dict[eras_list[eras_list.index(era) + 1]]["start_sync_time"]
            last_epoch = (
                int(era_details_dict[eras_list[eras_list.index(era) + 1]]["start_epoch"]) - 1
            )

        actual_era_dict = era_details_dict[era]
        actual_era_dict["last_epoch"] = last_epoch
        actual_era_dict["end_sync_time"] = end_sync_time

        no_of_epochs_in_era = (
            int(last_epoch)
            - int(era_details_dict[eras_list[eras_list.index(era)]]["start_epoch"])
            + 1
        )
        actual_era_dict["slots_in_era"] = get_no_of_slots_in_era(env, era, no_of_epochs_in_era)

        actual_era_dict["sync_duration_secs"] = int(
            (
                datetime.datetime.strptime(end_sync_time, "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=datetime.timezone.utc
                )
                - datetime.datetime.strptime(
                    actual_era_dict["start_sync_time"], "%Y-%m-%dT%H:%M:%SZ"
                ).replace(tzinfo=datetime.timezone.utc)
            ).total_seconds()
        )

        actual_era_dict["sync_speed_sps"] = int(
            actual_era_dict["slots_in_era"] / actual_era_dict["sync_duration_secs"]
        )

        era_details_dict[era] = actual_era_dict

    # calculate and add "end_sync_time" and "sync_duration_secs" for each epoch;
    epoch_list = list(epoch_details_dict.keys())
    for epoch in epoch_list:
        if epoch == epoch_list[-1]:
            epoch_end_sync_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        else:
            epoch_end_sync_time = epoch_details_dict[epoch_list[epoch_list.index(epoch) + 1]][
                "start_sync_time"
            ]
        actual_epoch_dict = epoch_details_dict[epoch]
        actual_epoch_dict["end_sync_time"] = epoch_end_sync_time
        actual_epoch_dict["sync_duration_secs"] = int(
            (
                datetime.datetime.strptime(epoch_end_sync_time, "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=datetime.timezone.utc
                )
                - datetime.datetime.strptime(
                    actual_epoch_dict["start_sync_time"], "%Y-%m-%dT%H:%M:%SZ"
                ).replace(tzinfo=datetime.timezone.utc)
            ).total_seconds()
        )
        epoch_details_dict[epoch] = actual_epoch_dict
    return (
        sync_time_seconds,
        last_slot_no,
        latest_chunk_no,
        era_details_dict,
        epoch_details_dict,
    )


def get_no_of_slots_in_era(env: str, era_name: str, no_of_epochs_in_era: int) -> int:
    slot_length_secs = 1
    epoch_length_slots = 432000

    if era_name.lower() == "byron":
        slot_length_secs = 20
    if env == "shelley-qa":
        epoch_length_slots = 7200
    if env == "preview":
        epoch_length_slots = 86400

    epoch_length_secs = int(epoch_length_slots / slot_length_secs)
    return int(epoch_length_secs * no_of_epochs_in_era)


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


def get_cabal_build_files(repo_dir: pl.Path) -> list[pl.Path]:
    build_dir = repo_dir / "dist-newstyle/build"

    node_build_files: list[pl.Path] = []
    if build_dir.exists():
        node_build_files.extend(f.resolve() for f in build_dir.rglob("*") if f.is_file())

    return node_build_files


def get_node_executable_path_built_with_cabal(repo_dir: pl.Path) -> pl.Path | None:
    for f in get_cabal_build_files(repo_dir=repo_dir):
        if (
            "x" in f.parts
            and "cardano-node" in f.parts
            and "build" in f.parts
            and "cardano-node-tmp" not in f.name
            and "autogen" not in f.name
        ):
            return f
    return None


def get_cli_executable_path_built_with_cabal(repo_dir: pl.Path) -> pl.Path | None:
    for f in get_cabal_build_files(repo_dir=repo_dir):
        if (
            "x" in f.parts
            and "cardano-cli" in f.parts
            and "build" in f.parts
            and "cardano-cli-tmp" not in f.name
            and "autogen" not in f.name
        ):
            return f
    return None


def copy_cabal_node_exe(repo_dir: pl.Path, dst_location: pl.Path) -> None:
    node_binary_location_tmp = get_node_executable_path_built_with_cabal(repo_dir=repo_dir)
    assert node_binary_location_tmp is not None  # TODO: refactor
    node_binary_location = node_binary_location_tmp
    shutil.copy2(node_binary_location, dst_location / "cardano-node")
    make_executable(path=dst_location / "cardano-node")


def copy_cabal_cli_exe(repo_dir: pl.Path, dst_location: pl.Path) -> None:
    cli_binary_location_tmp = get_cli_executable_path_built_with_cabal(repo_dir=repo_dir)
    assert cli_binary_location_tmp is not None  # TODO: refactor
    cli_binary_location = cli_binary_location_tmp
    shutil.copy2(cli_binary_location, dst_location / "cardano-cli")
    make_executable(path=dst_location / "cardano-cli")


def ln_nix_node_from_repo(repo_dir: pl.Path, dst_location: pl.Path) -> None:
    (dst_location / "cardano-node").unlink(missing_ok=True)  # Remove existing file if any
    os.symlink(
        repo_dir / "cardano-node-bin" / "bin" / "cardano-node",
        dst_location / "cardano-node",
    )

    (dst_location / "cardano-cli").unlink(missing_ok=True)  # Remove existing file if any
    os.symlink(
        repo_dir / "cardano-cli-bin" / "bin" / "cardano-cli",
        dst_location / "cardano-cli",
    )


def get_node_repo(node_rev: str) -> git.Repo:
    node_repo_name = "cardano-node"
    node_repo_dir = pl.Path("cardano_node_dir")

    if node_repo_dir.is_dir():
        repo = git.Repo(path=node_repo_dir)
        gitpython.git_checkout(repo, node_rev)
    else:
        repo = gitpython.git_clone_iohk_repo(node_repo_name, node_repo_dir, node_rev)

    return repo


def get_cli_repo(cli_rev: str) -> git.Repo:
    node_repo_name = "cardano-cli"
    cli_repo_dir = pl.Path("cardano_cli_dir")

    if cli_repo_dir.is_dir():
        repo = git.Repo(path=cli_repo_dir)
        gitpython.git_checkout(repo, cli_rev)
    else:
        repo = gitpython.git_clone_iohk_repo(node_repo_name, cli_repo_dir, cli_rev)

    return repo


def get_node_files(node_rev: str, build_tool: str = "nix") -> git.Repo:
    test_directory = pl.Path.cwd()

    node_repo = get_node_repo(node_rev=node_rev)
    node_repo_dir = pl.Path(node_repo.git_dir)

    if build_tool == "nix":
        with temporary_chdir(path=node_repo_dir):
            pl.Path("cardano-node-bin").unlink(missing_ok=True)
            pl.Path("cardano-cli-bin").unlink(missing_ok=True)
            helpers.execute_command("nix build -v .#cardano-node -o cardano-node-bin")
            helpers.execute_command("nix build -v .#cardano-cli -o cardano-cli-bin")
        ln_nix_node_from_repo(repo_dir=node_repo_dir, dst_location=test_directory)

    elif build_tool == "cabal":
        cabal_local_file = pl.Path(test_directory) / "sync_tests" / "cabal.project.local"
        cli_repo = get_cli_repo(cli_rev="main")
        cli_repo_dir = pl.Path(cli_repo.git_dir)

        # Build cli
        with temporary_chdir(path=cli_repo_dir):
            shutil.copy2(cabal_local_file, cli_repo_dir)
            LOGGER.debug(f" - listdir cli_repo_dir: {os.listdir(cli_repo_dir)}")
            shutil.rmtree("dist-newstyle", ignore_errors=True)
            for line in fileinput.input("cabal.project", inplace=True):
                LOGGER.debug(line.replace("tests: True", "tests: False"))
            helpers.execute_command("cabal update")
            helpers.execute_command("cabal build cardano-cli")
        copy_cabal_cli_exe(repo_dir=cli_repo_dir, dst_location=test_directory)
        gitpython.git_checkout(cli_repo, "cabal.project")

        # Build node
        with temporary_chdir(path=node_repo_dir):
            shutil.copy2(cabal_local_file, node_repo_dir)
            LOGGER.debug(f" - listdir node_repo_dir: {os.listdir(node_repo_dir)}")
            shutil.rmtree("dist-newstyle", ignore_errors=True)
            for line in fileinput.input("cabal.project", inplace=True):
                LOGGER.debug(line.replace("tests: True", "tests: False"))
            helpers.execute_command("cabal update")
            helpers.execute_command("cabal build cardano-node")
        copy_cabal_cli_exe(repo_dir=node_repo_dir, dst_location=test_directory)
        gitpython.git_checkout(node_repo, "cabal.project")

    return node_repo


def config_sync(
    env: str,
    conf_dir: pl.Path,
    node_build_mode: str,
    node_rev: str,
    node_topology_type: str,
    use_genesis_mode: bool,
) -> None:
    LOGGER.info(f"Get the cardano-node and cardano-cli files using - {node_build_mode}")
    start_build_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")

    platform_system = platform.system().lower()
    if "windows" not in platform_system:
        get_node_files(node_rev)
    elif "windows" in platform_system:
        get_node_files(node_rev, build_tool="cabal")
    else:
        err = f"Only building with NIX is supported at this moment - {node_build_mode}"
        raise exceptions.SyncError(err)

    end_build_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
    LOGGER.info(f"  - start_build_time: {start_build_time}")
    LOGGER.info(f"  - end_build_time: {end_build_time}")

    rm_node_config_files(conf_dir=conf_dir)
    # TODO: change the default to P2P when full P2P will be supported on Mainnet
    get_node_config_files(
        env=env,
        node_topology_type=node_topology_type,
        conf_dir=conf_dir,
        use_genesis_mode=use_genesis_mode,
    )

    configure_node(config_file=conf_dir / "config.json")
    if env == "mainnet" and node_topology_type == "legacy":
        disable_p2p_node_config(config_file=conf_dir / "config.json")


def run_sync(node_start_arguments: tp.Iterable[str], base_dir: pl.Path, env: str) -> SyncRec | None:
    if "None" in node_start_arguments:
        node_start_arguments = []

    start_sync_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")

    node_proc = None
    logfile = None
    try:
        node_proc, logfile = start_node(
            cardano_node=NODE, base_dir=base_dir, node_start_arguments=node_start_arguments
        )
        secs_to_start = wait_node_start(env=env, timeout_minutes=10)
        (
            sync_time_seconds,
            last_slot_no,
            latest_chunk_no,
            era_details_dict,
            epoch_details_dict,
        ) = wait_for_node_to_sync(env=env, base_dir=base_dir)
    except Exception:
        LOGGER.exception("Could not finish sync.")
        return None
    finally:
        end_sync_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime(
            "%d/%m/%Y %H:%M:%S"
        )
        if node_proc:
            node_status = get_node_exit_code(proc=node_proc)
            if node_status != -1:
                LOGGER.error(f"Node exited unexpectedly with code: {node_status}")
            else:
                exit_code = stop_node(proc=node_proc)
                LOGGER.warning(f"Node stopped with exit code: {exit_code}")
        if logfile:
            logfile.flush()
            logfile.close()

    return SyncRec(
        secs_to_start=secs_to_start,
        sync_time_sec=sync_time_seconds,
        last_slot_no=last_slot_no,
        latest_chunk_no=latest_chunk_no,
        era_details=era_details_dict,
        epoch_details=epoch_details_dict,
        start_sync_time=start_sync_time,
        end_sync_time=end_sync_time,
    )


def run_test(args: argparse.Namespace) -> None:
    conf_dir = pl.Path.cwd()
    base_dir = pl.Path.cwd()

    set_node_socket_path_env_var(base_dir=base_dir)

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
    config_sync(
        env=env,
        conf_dir=conf_dir,
        node_build_mode=node_build_mode,
        node_rev=node_rev1,
        node_topology_type=node_topology_type1,
        use_genesis_mode=use_genesis_mode,
    )

    helpers.print_message(type="warn", message="--- node version ")
    cli_version1, cli_git_rev1 = get_node_version()
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
    sync1_rec = run_sync(node_start_arguments=node_start_arguments1, base_dir=base_dir, env=env)
    if not sync1_rec:
        sys.exit(1)

    print(f"secs_to_start1: {sync1_rec.secs_to_start}")
    print(f"start_sync_time1: {sync1_rec.start_sync_time}")
    print(f"end_sync_time1: {sync1_rec.end_sync_time}")

    # we are interested in the node logs only for the main sync - using tag_no1
    test_values_dict: dict[str, tp.Any] = {}
    print("--- Parse the node logs and get the relevant data")
    logs_details_dict = get_data_from_logs(log_file=base_dir / NODE_LOG_FILE_NAME)
    test_values_dict["log_values"] = logs_details_dict

    sync2_rec = None
    print(f"--- Start node using tag_no2: {tag_no2}")
    if tag_no2 and tag_no2 != "None":
        delete_node_files()
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
        config_sync(
            env=env,
            conf_dir=conf_dir,
            node_build_mode=node_build_mode,
            node_rev=node_rev2,
            node_topology_type=node_topology_type2,
            use_genesis_mode=use_genesis_mode,
        )

        helpers.print_message(type="warn", message="node version")
        cli_version2, cli_git_rev2 = get_node_version()
        print(f" - cardano_cli_version2: {cli_version2}")
        print(f" - cardano_cli_git_rev2: {cli_git_rev2}")
        print()
        print(f"================ Start node using node_rev2: {node_rev2} ====================")
        sync2_rec = run_sync(node_start_arguments=node_start_arguments2, base_dir=base_dir, env=env)
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
    test_values_dict["epoch_no_d_zero"] = get_epoch_no_d_zero(env=env)
    test_values_dict["start_slot_no_d_zero"] = get_start_slot_no_d_zero(env=env)
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
    logging.setLoggerClass(ColorLogger)
    args = get_args()
    run_test(args=args)

    return 0


if __name__ == "__main__":
    sys.exit(main())
