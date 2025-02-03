import argparse
import contextlib
import datetime
import fileinput
import json
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

import git

import sync_tests.utils.helpers as utils
from sync_tests.utils import blockfrost
from sync_tests.utils import explorer
from sync_tests.utils import gitpython

CONFIGS_BASE_URL = "https://book.play.dev.cardano.org/environments"
NODE = pl.Path.cwd() / "cardano-node"
CLI = pl.Path.cwd() / "cardano-cli"
NODE_LOG_FILE_NAME = "logfile.log"
NODE_LOG_FILE_ARTIFACT = "node.log"
RESULTS_FILE_NAME = "sync_results.json"


@contextlib.contextmanager
def temporary_chdir(path: pl.Path) -> tp.Iterator[None]:
    prev_cwd = pl.Path.cwd()  # Store the current working directory
    try:
        os.chdir(path)  # Change to the new directory
        yield
    finally:
        os.chdir(prev_cwd)  # Restore the original working directory


def delete_node_files() -> None:
    for p in pl.Path("..").glob("cardano-*"):
        if p.is_dir():
            utils.print_message(type="info_warn", message=f"deleting directory: {p}")
            shutil.rmtree(p)  # Use shutil.rmtree to delete directories
        else:
            utils.print_message(type="info_warn", message=f"deleting file: {p}")
            p.unlink(missing_ok=True)


def make_executable(path: pl.Path) -> None:
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def rm_node_config_files(conf_dir: pl.Path) -> None:
    utils.print_message(type="info_warn", message="Removing existing config files")
    for gen in conf_dir.glob("*-genesis.json"):
        pl.Path(gen).unlink(missing_ok=True)
    for f in ("config.json", "topology.json"):
        (conf_dir / f).unlink(missing_ok=True)


def download_config_file(config_slug: str, save_as: pl.Path) -> None:
    url = f"{CONFIGS_BASE_URL}/{config_slug}"
    print(f"Downloading {url} and saving as {save_as}...")
    urllib.request.urlretrieve(url, save_as)


def get_node_config_files(env: str, node_topology_type: str, conf_dir: pl.Path) -> None:
    download_config_file(config_slug=f"{env}/config.json", save_as=conf_dir / "config.json")
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
            save_as=conf_dir / "topology.json",
        )
    elif env == "mainnet" and node_topology_type == "legacy":
        download_config_file(
            config_slug=f"{env}/topology-legacy.json", save_as=conf_dir / "topology.json"
        )
    else:
        download_config_file(config_slug=f"{env}/topology.json", save_as=conf_dir / "topology.json")


def configure_node(config_file: pl.Path) -> None:
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
    with open(config_file) as json_file:
        node_config_json = json.load(json_file)

    # Use the legacy topology
    node_config_json["EnableP2P"] = False
    node_config_json["PeerSharing"] = False

    with open(config_file, "w") as json_file:
        json.dump(node_config_json, json_file, indent=2)


def set_node_socket_path_env_var(base_dir: pl.Path) -> None:
    socket_path: str | pl.Path
    if "windows" in platform.system().lower():
        socket_path = "\\\\.\\pipe\\cardano-node"
    else:
        start_socket_path = os.environ.get("CARDANO_NODE_SOCKET_PATH")
        if start_socket_path is None:
            socket_path = (base_dir / "db" / "node.socket").expanduser().absolute()
        else:
            socket_path = pl.Path(start_socket_path).expanduser().absolute()
    os.environ["CARDANO_NODE_SOCKET_PATH"] = str(socket_path)


def get_epoch_no_d_zero() -> int:
    env = utils.get_arg_value(args=args, key="environment")
    if env == "mainnet":
        return 257
    return -1


def get_start_slot_no_d_zero() -> int:
    env = utils.get_arg_value(args=args, key="environment")
    if env == "mainnet":
        return 25661009
    return -1


def get_testnet_value() -> str:
    env = utils.get_arg_value(args=args, key="environment")
    arg = ""
    if env == "mainnet":
        arg = "--mainnet"
    if env == "preview":
        arg = "--testnet-magic 2"
    if env == "preprod":
        arg = "--testnet-magic 1"
    return arg


def get_current_tip() -> tuple:
    cmd = f"{CLI} latest query tip {get_testnet_value()}"

    output = (
        subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT).decode("utf-8").strip()
    )
    output_json = json.loads(output)
    epoch = int(output_json.get("epoch", 0))
    block = int(output_json.get("block", 0))
    hash_value = output_json.get("hash", "")
    slot = int(output_json.get("slot", 0))
    era = output_json.get("era", "").lower()
    sync_progress = (
        int(float(output_json.get("syncProgress", 0.0))) if "syncProgress" in output_json else None
    )

    return epoch, block, hash_value, slot, era, sync_progress


def wait_query_tip_available(timeout_minutes: int = 20) -> int:
    # when starting from clean state it might take ~30 secs for the cli to work
    # when starting from existing state it might take > 10 mins for the cli to work (opening db and
    # replaying the ledger)
    start_counter = time.perf_counter()

    for i in range(timeout_minutes):
        try:
            get_current_tip()
            break
        except subprocess.CalledProcessError as e:
            now = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
            print(f" === {now} - Waiting 60s before retrying to get the tip again - {i}")
            utils.print_message(
                type="error",
                message=(
                    f"     !!! ERROR: command {e.cmd} returned with error "
                    f"(code {e.returncode}): {' '.join(str(e.output.decode('utf-8')).split())}"
                ),
            )
            if "Invalid argument" in str(e.output):
                print(f" -- exiting on - {e.output.decode('utf-8')}")
                sys.exit(1)
        time.sleep(60)
    else:
        utils.print_message(type="error", message="     !!! ERROR: failed to get tip")
        sys.exit(1)

    stop_counter = time.perf_counter()
    start_time_seconds = int(stop_counter - start_counter)
    utils.print_message(
        type="ok",
        message=f"It took {start_time_seconds} seconds for the QUERY TIP command to be available",
    )
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

    utils.print_message(type="info_warn", message=f"start node cmd: {cmd}")
    logfile = open(base_dir / NODE_LOG_FILE_NAME, "w+")

    proc = subprocess.Popen(cmd.split(" "), stdout=logfile, stderr=logfile)
    return proc, logfile


def wait_node_start(timeout_minutes: int = 20) -> int:
    current_directory = pl.Path.cwd()

    utils.print_message(type="info", message="waiting for db folder to be created")
    count = 0
    count_timeout = 299
    while not pl.Path.is_dir(current_directory / "db"):
        time.sleep(1)
        count += 1
        if count > count_timeout:
            utils.print_message(
                type="error",
                message=(
                    f"ERROR: waited {count_timeout} seconds and the DB folder was not created yet"
                ),
            )
            sys.exit(1)

    utils.print_message(type="ok", message=f"DB folder was created after {count} seconds")
    secs_to_start = wait_query_tip_available(timeout_minutes)
    print(f" - listdir current_directory: {os.listdir(current_directory)}")
    print(f" - listdir db: {os.listdir(current_directory / 'db')}")
    return secs_to_start


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
    era_details_dict = {}
    epoch_details_dict = {}

    actual_epoch, actual_block, actual_hash, actual_slot, actual_era, sync_progress = (
        get_current_tip()
    )
    last_slot_no = get_calculated_slot_no(env)
    start_sync = time.perf_counter()

    count = 0
    if sync_progress is not None:
        while sync_progress < 100:
            if count % 60 == 0:
                now = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
                utils.print_message(
                    type="warn",
                    message=f"{now} - actual_era  : {actual_era} "
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
            ) = get_current_tip()

    else:
        while actual_slot <= last_slot_no:
            if count % 60 == 0:
                now = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
                utils.print_message(
                    type="warn",
                    message=f"{now} - actual_era  : {actual_era} "
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
            ) = get_current_tip()

    end_sync = time.perf_counter()
    sync_time_seconds = int(end_sync - start_sync)
    print(f"sync_time_seconds: {sync_time_seconds}")

    chunk_files = sorted((base_dir / "db" / "immutable").iterdir(), key=lambda f: f.stat().st_mtime)
    latest_chunk_no = chunk_files[-1].stem
    utils.print_message(type="ok", message=f"Sync done!; latest_chunk_no: {latest_chunk_no}")

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


# TODO: refactor completely
def get_node_files(
    node_rev: str, repository: git.Repo | None = None, build_tool: str = "nix"
) -> git.Repo:
    test_directory = pl.Path.cwd()
    repo = None

    node_repo_name = "cardano-node"
    node_repo_dir = pl.Path("cardano_node_dir")

    if node_repo_dir.is_dir() and repository:
        repo = gitpython.git_checkout(repository, node_rev)
    else:
        repo = gitpython.git_clone_iohk_repo(node_repo_name, node_repo_dir, node_rev)

    if build_tool == "nix":
        with temporary_chdir(path=node_repo_dir):
            pl.Path("cardano-node-bin").unlink(missing_ok=True)
            pl.Path("cardano-cli-bin").unlink(missing_ok=True)
            utils.execute_command("nix build -v .#cardano-node -o cardano-node-bin")
            utils.execute_command("nix build -v .#cardano-cli -o cardano-cli-bin")
        ln_nix_node_from_repo(repo_dir=node_repo_dir, dst_location=test_directory)

    elif build_tool == "cabal":
        cabal_local_file = pl.Path(test_directory) / "sync_tests" / "cabal.project.local"

        cli_rev = "main"
        cli_repo_name = "cardano-cli"
        cli_repo_dir = test_directory / "cardano_cli_dir"

        if cli_repo_dir.is_dir() and repository:
            gitpython.git_checkout(repository, cli_rev)
        else:
            gitpython.git_clone_iohk_repo(cli_repo_name, cli_repo_dir, cli_rev)

        # Build cli
        with temporary_chdir(path=cli_repo_dir):
            shutil.copy2(cabal_local_file, cli_repo_dir)
            print(f" - listdir cli_repo_dir: {os.listdir(cli_repo_dir)}")
            shutil.rmtree("dist-newstyle", ignore_errors=True)
            for line in fileinput.input("cabal.project", inplace=True):
                print(line.replace("tests: True", "tests: False"), end="")
            utils.execute_command("cabal update")
            utils.execute_command("cabal build cardano-cli")
        copy_cabal_cli_exe(repo_dir=cli_repo_dir, dst_location=test_directory)

        # Build node
        with temporary_chdir(path=node_repo_dir):
            shutil.copy2(cabal_local_file, node_repo_dir)
            print(f" - listdir node_repo_dir: {os.listdir(node_repo_dir)}")
            shutil.rmtree("dist-newstyle", ignore_errors=True)
            for line in fileinput.input("cabal.project", inplace=True):
                print(line.replace("tests: True", "tests: False"), end="")
            utils.execute_command("cabal update")
            utils.execute_command("cabal build cardano-node")
        copy_cabal_cli_exe(repo_dir=node_repo_dir, dst_location=test_directory)
        gitpython.git_checkout(repo, "cabal.project")

    return repo


def main() -> None:
    repository = None
    secs_to_start1, secs_to_start2 = 0, 0

    conf_dir = pl.Path.cwd()
    base_dir = pl.Path.cwd()

    set_node_socket_path_env_var(base_dir=base_dir)

    print("--- Test data information", flush=True)
    start_test_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
    utils.print_message(type="info", message=f"Test start time: {start_test_time}")
    utils.print_message(type="warn", message="Test parameters:")
    env = utils.get_arg_value(args=args, key="environment")
    node_build_mode = utils.get_arg_value(args=args, key="build_mode")
    node_rev1 = utils.get_arg_value(args=args, key="node_rev1")
    node_rev2 = utils.get_arg_value(args=args, key="node_rev2")
    tag_no1 = utils.get_arg_value(args=args, key="tag_no1")
    tag_no2 = utils.get_arg_value(args=args, key="tag_no2")
    node_topology_type1 = utils.get_arg_value(args=args, key="node_topology1")
    node_topology_type2 = utils.get_arg_value(args=args, key="node_topology2")
    node_start_arguments1 = utils.get_arg_value(args=args, key="node_start_arguments1")
    node_start_arguments2 = utils.get_arg_value(args=args, key="node_start_arguments2")

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

    platform_system, platform_release, platform_version = utils.get_os_type()
    print(f"- platform: {platform_system, platform_release, platform_version}")

    print("--- Get the cardano-node files", flush=True)
    utils.print_message(
        type="info",
        message=f"Get the cardano-node and cardano-cli files using - {node_build_mode}",
    )
    start_build_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
    if "windows" not in platform_system.lower():
        repository = get_node_files(node_rev1)
    elif "windows" in platform_system.lower():
        repository = get_node_files(node_rev1, build_tool="cabal")
    else:
        utils.print_message(
            type="error",
            message=(
                f"ERROR: method not implemented yet!!! "
                f"Only building with NIX is supported at this moment - {node_build_mode}"
            ),
        )
        sys.exit(1)
    end_build_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
    utils.print_message(type="info", message=f"  - start_build_time: {start_build_time}")
    utils.print_message(type="info", message=f"  - end_build_time: {end_build_time}")

    utils.print_message(type="warn", message="--- node version ")
    cli_version1, cli_git_rev1 = get_node_version()
    print(f"  - cardano_cli_version1: {cli_version1}")
    print(f"  - cardano_cli_git_rev1: {cli_git_rev1}")

    print("--- Get the node configuration files")
    rm_node_config_files(conf_dir=conf_dir)
    # TO DO: change the default to P2P when full P2P will be supported on Mainnet
    get_node_config_files(env=env, node_topology_type=node_topology_type1, conf_dir=conf_dir)

    print("Configure node")
    configure_node(config_file=conf_dir / "config.json")
    if env == "mainnet" and node_topology_type1 == "legacy":
        disable_p2p_node_config(config_file=conf_dir / "config.json")

    print(f"--- Start node sync test using node_rev1: {node_rev1}")
    utils.print_message(
        type="ok",
        message="===================================================================================",
    )
    utils.print_message(
        type="ok",
        message=(
            f"================== Start node sync test using node_rev1: {node_rev1} ============="
        ),
    )
    utils.print_message(
        type="ok",
        message="===================================================================================",
    )
    print()
    start_sync_time1 = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
    if "None" in node_start_arguments1:
        node_start_arguments1 = []
    node_proc1, logfile1 = start_node(
        cardano_node=NODE, base_dir=base_dir, node_start_arguments=node_start_arguments1
    )
    secs_to_start1 = wait_node_start(timeout_minutes=10)

    utils.print_message(type="info", message=" - waiting for the node to sync")

    sync1_error = False
    try:
        (
            sync_time_seconds1,
            last_slot_no1,
            latest_chunk_no1,
            era_details_dict1,
            epoch_details_dict1,
        ) = wait_for_node_to_sync(env=env, base_dir=base_dir)
    except Exception as e:
        sync1_error = True
        utils.print_message(
            type="error",
            message=f" !!! ERROR - could not finish sync1 - {e}",
        )
    finally:
        end_sync_time1 = datetime.datetime.now(tz=datetime.timezone.utc).strftime(
            "%d/%m/%Y %H:%M:%S"
        )
        utils.print_message(type="warn", message=f"Stop node for: {node_rev1}")
        exit_code1 = stop_node(proc=node_proc1)
        utils.print_message(type="warn", message=f"Exit code: {exit_code1}")
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
        delete_node_files()
        print()
        utils.print_message(
            type="ok",
            message="==============================================================================",
        )
        utils.print_message(
            type="ok",
            message=(
                f"================= Start sync using node_rev2: {node_rev2} ==================="
            ),
        )
        utils.print_message(
            type="ok",
            message="==============================================================================",
        )

        print("Get the cardano-node and cardano-cli files")
        if "windows" not in platform_system.lower():
            get_node_files(node_rev2, repository)
        elif "windows" in platform_system.lower():
            get_node_files(node_rev2, repository, build_tool="cabal")
        else:
            utils.print_message(
                type="error",
                message=(
                    "ERROR: method not implemented yet!!! "
                    f"Only building with NIX is supported at this moment - {node_build_mode}"
                ),
            )
            sys.exit(1)

        if env == "mainnet" and (node_topology_type1 != node_topology_type2):
            utils.print_message(type="warn", message="remove the previous topology")
            (conf_dir / "topology.json").unlink()
            print("Getting the node configuration files")
            get_node_config_files(
                env=env, node_topology_type=node_topology_type2, conf_dir=conf_dir
            )

        utils.print_message(type="warn", message="node version")
        cli_version2, cli_git_rev2 = get_node_version()
        print(f" - cardano_cli_version2: {cli_version2}")
        print(f" - cardano_cli_git_rev2: {cli_git_rev2}")
        print()
        print(f"================ Start node using node_rev2: {node_rev2} ====================")
        start_sync_time2 = datetime.datetime.now(tz=datetime.timezone.utc).strftime(
            "%d/%m/%Y %H:%M:%S"
        )
        if "None" in node_start_arguments1:
            node_start_arguments2 = []
        node_proc2, logfile2 = start_node(
            cardano_node=NODE, base_dir=base_dir, node_start_arguments=node_start_arguments2
        )
        secs_to_start2 = wait_node_start()

        utils.print_message(
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
            ) = wait_for_node_to_sync(env=env, base_dir=base_dir)
        except Exception as e:
            sync2_error = True
            utils.print_message(
                type="error",
                message=f" !!! ERROR - could not finish sync2 - {e}",
            )
        finally:
            end_sync_time2 = datetime.datetime.now(tz=datetime.timezone.utc).strftime(
                "%d/%m/%Y %H:%M:%S"
            )
            utils.print_message(type="warn", message=f"Stop node for: {node_rev2}")
            exit_code2 = stop_node(proc=node_proc2)
            utils.print_message(type="warn", message=f"Exit code: {exit_code2}")
            logfile2.flush()
            logfile2.close()

        if sync2_error:
            sys.exit(1)

    chain_size = utils.get_directory_size(base_dir / "db")

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
    test_values_dict["total_ram_in_GB"] = utils.get_total_ram_in_gb()
    test_values_dict["epoch_no_d_zero"] = get_epoch_no_d_zero()
    test_values_dict["start_slot_no_d_zero"] = get_start_slot_no_d_zero()
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


if __name__ == "__main__":
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

    args = parser.parse_args()

    main()
