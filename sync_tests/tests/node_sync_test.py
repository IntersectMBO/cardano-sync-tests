import argparse
import fileinput
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import time
import urllib.request
from collections import OrderedDict
from datetime import datetime
from datetime import timedelta
from pathlib import Path

from psutil import process_iter
from typing import Optional

sys.path.append(os.getcwd())

from sync_tests.utils import blockfrost
from sync_tests.utils import explorer
from sync_tests.utils import gitpython
from sync_tests.utils import helpers
from sync_tests.utils import node


CONFIGS_BASE_URL = "https://book.play.dev.cardano.org/environments"
NODE = "./cardano-node"
CLI = "./cardano-cli"
ROOT_TEST_PATH = ""
NODE_LOG_FILE = "logfile.log"
NODE_LOG_FILE_ARTIFACT = "node.log"
RESULTS_FILE_NAME = r"sync_results.json"
ONE_MINUTE = 60


def set_repo_paths():
    global ROOT_TEST_PATH
    ROOT_TEST_PATH = Path.cwd()
    print(f"ROOT_TEST_PATH: {ROOT_TEST_PATH}")


def delete_node_files():
    for p in Path("..").glob("cardano-*"):
        if p.is_dir():
            helpers.print_message(type="info_warn", message=f"deleting directory: {p}")
            shutil.rmtree(p)  # Use shutil.rmtree to delete directories
        else:
            helpers.print_message(type="info_warn", message=f"deleting file: {p}")
            p.unlink(missing_ok=True)


def update_config(file_name: str, updates: dict) -> None:
    # Load the current configuration from the JSON file
    with open(file_name) as file:
        config = json.load(file)

    # Update the config with the new values
    for key, value in updates.items():
        if key in config:
            print(f"Updating '{key}' from '{config[key]}' to '{value}'")
            config[key] = value
        else:
            print(f"Key '{key}' not found in the config, adding new key-value pair")
            config[key] = value

    # Write the updated configuration back to the JSON file
    with open(file_name, "w") as file:
        json.dump(config, file, indent=4)
    print("Configuration updated successfully.")


def disable_p2p_node_config():
    os.chdir(Path(ROOT_TEST_PATH))
    current_directory = Path.cwd()
    print(f"current_directory: {current_directory}")
    print(f" - listdir current_directory: {os.listdir(current_directory)}")

    updates = {"EnableP2P": False, "PeerSharing": False}
    update_config("config.json", updates)


def rm_node_config_files() -> None:
    helpers.print_message(type="info_warn", message="Removing existing config files")
    os.chdir(Path(ROOT_TEST_PATH))
    for gen in Path("..").glob("*-genesis.json"):
        Path(gen).unlink(missing_ok=True)
    for f in ("config.json", "topology.json"):
        Path(f).unlink(missing_ok=True)


def download_config_file(env: str, file_name: str, save_as: Optional[str] = None) -> None:
    save_as = save_as or file_name
    url = f"{CONFIGS_BASE_URL}/{env}/{file_name}"
    print(f"Downloading {file_name} from {url} and saving as {save_as}...")
    urllib.request.urlretrieve(url, save_as)


def enable_cardano_node_resources_monitoring(node_config_filepath):
    helpers.print_message(type="warn", message="- Enable cardano node resource monitoring:")
    helpers.print_message(
        type="info",
        message='  node_config_json["options"]["mapBackends"]["cardano.node.resources"] = ["KatipBK"]',
    )

    os.chdir(Path(ROOT_TEST_PATH))
    with open(node_config_filepath) as json_file:
        node_config_json = json.load(json_file)
    node_config_json["options"]["mapBackends"]["cardano.node.resources"] = ["KatipBK"]
    with open(node_config_filepath, "w") as json_file:
        json.dump(node_config_json, json_file, indent=2)


def enable_cardano_node_tracers(node_config_filepath):
    os.chdir(Path(ROOT_TEST_PATH))
    with open(node_config_filepath) as json_file:
        node_config_json = json.load(json_file)

    helpers.print_message(type="warn", message="- Enable tracer:")
    helpers.print_message(type="info", message="  Set minSeverity = Info")
    node_config_json["minSeverity"] = "Info"

    with open(node_config_filepath, "w") as json_file:
        json.dump(node_config_json, json_file, indent=2)


def set_node_socket_path_env_var():
    if "windows" in platform.system().lower():
        socket_path = "\\\\.\\pipe\\cardano-node"
    else:
        socket_path = (Path(ROOT_TEST_PATH) / "db" / "node.socket").expanduser().absolute()
    os.environ["CARDANO_NODE_SOCKET_PATH"] = str(socket_path)


def get_epoch_no_d_zero():
    env = helpers.get_arg_value(args=args, key="environment")
    if env == "mainnet":
        return 257
    if env == "testnet":
        return 121
    if env == "staging":
        return None
    if env == "shelley-qa":
        return 2554
    return None


def get_start_slot_no_d_zero():
    env = helpers.get_arg_value(args=args, key="environment")
    if env == "mainnet":
        return 25661009
    if env == "testnet":
        return 21902400
    if env == "staging":
        return None
    if env == "shelley-qa":
        return 18375135
    return None


def stop_node(platform_system):
    for proc in process_iter():
        if "cardano-node" in proc.name():
            helpers.print_message(
                type="info_warn", message=f"Killing the `cardano-node` process - {proc}"
            )
            if "windows" in platform_system.lower():
                proc.send_signal(signal.SIGTERM)
            else:
                proc.send_signal(signal.SIGINT)
    time.sleep(20)
    for proc in process_iter():
        if "cardano-node" in proc.name():
            helpers.print_message(
                type="error",
                message=f" !!! ERROR: `cardano-node` process is still active - {proc}",
            )


def copy_log_file_artifact(old_name, new_name):
    os.chdir(Path(ROOT_TEST_PATH))
    current_directory = Path.cwd()
    print(f"current_directory: {current_directory}")
    helpers.execute_command(f"cp {old_name} {new_name}")
    print(f" - listdir current_directory: {os.listdir(current_directory)}")


def get_calculated_slot_no(env):
    current_time = datetime.utcnow()
    shelley_start_time = byron_start_time = current_time

    if env == "testnet":
        byron_start_time = datetime.strptime("2019-07-24 20:20:16", "%Y-%m-%d %H:%M:%S")
        shelley_start_time = datetime.strptime("2020-07-28 20:20:16", "%Y-%m-%d %H:%M:%S")
    elif env == "staging":
        byron_start_time = datetime.strptime("2017-09-26 18:23:33", "%Y-%m-%d %H:%M:%S")
        shelley_start_time = datetime.strptime("2020-08-01 18:23:33", "%Y-%m-%d %H:%M:%S")
    elif env == "mainnet":
        byron_start_time = datetime.strptime("2017-09-23 21:44:51", "%Y-%m-%d %H:%M:%S")
        shelley_start_time = datetime.strptime("2020-07-29 21:44:51", "%Y-%m-%d %H:%M:%S")
    elif env == "shelley-qa":
        byron_start_time = datetime.strptime("2020-08-17 13:00:00", "%Y-%m-%d %H:%M:%S")
        shelley_start_time = datetime.strptime("2020-08-17 17:00:00", "%Y-%m-%d %H:%M:%S")
    elif env == "preprod":
        byron_start_time = datetime.strptime("2022-06-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        shelley_start_time = datetime.strptime("2022-06-21 00:00:00", "%Y-%m-%d %H:%M:%S")
    elif env == "preview":
        # this env was started directly in Alonzo
        byron_start_time = datetime.strptime("2022-08-09 00:00:00", "%Y-%m-%d %H:%M:%S")
        shelley_start_time = datetime.strptime("2022-08-09 00:00:00", "%Y-%m-%d %H:%M:%S")

    last_slot_no = int(
        (shelley_start_time - byron_start_time).total_seconds() / 20
        + (current_time - shelley_start_time).total_seconds()
    )
    return last_slot_no


def get_no_of_slots_in_era(env, era_name, no_of_epochs_in_era):
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


def get_data_from_logs(log_file):
    os.chdir(Path(ROOT_TEST_PATH))
    current_directory = Path.cwd()
    print(f"current_directory: {current_directory}")

    tip_details_dict = OrderedDict()
    heap_ram_details_dict = OrderedDict()
    rss_ram_details_dict = OrderedDict()
    centi_cpu_dict = OrderedDict()
    cpu_details_dict = OrderedDict()
    logs_details_dict = OrderedDict()

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

    no_of_cpu_cores = os.cpu_count()
    timestamps_list = list(centi_cpu_dict.keys())
    for timestamp1 in timestamps_list[1:]:
        # %CPU = dValue / dt for 1 core
        previous_timestamp = datetime.strptime(
            timestamps_list[timestamps_list.index(timestamp1) - 1], "%Y-%m-%d %H:%M:%S"
        )
        current_timestamp = datetime.strptime(
            timestamps_list[timestamps_list.index(timestamp1)], "%Y-%m-%d %H:%M:%S"
        )
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


def get_cabal_build_files():
    node_build_files = []

    for dirpath, _, filenames in os.walk("dist-newstyle/build"):
        for f in filenames:
            abs_filepath = os.path.abspath(os.path.join(dirpath, f))
            node_build_files.append(abs_filepath)
    return node_build_files


def get_node_executable_path_built_with_cabal():
    for f in get_cabal_build_files():
        if "\\x\\cardano-node\\build\\" in f and "cardano-node-tmp" not in f and "autogen" not in f:
            helpers.print_message(type="info", message=f"Found node executable: {f}")
            global NODE
            NODE = f
            return f
    return None


def get_cli_executable_path_built_with_cabal():
    for f in get_cabal_build_files():
        if "\\x\\cardano-cli\\build\\" in f and "cardano-cli-tmp" not in f and "autogen" not in f:
            help().print_message(type="info", message=f"Found node-cli executable: {f}")
            global CLI
            CLI = f
            return f
    return None


def get_node_files(node_rev, repository=None, build_tool="nix"):
    test_directory = Path.cwd()
    repo = None
    helpers.print_message(type="info", message=f"test_directory: {test_directory}")
    print(f" - listdir test_directory: {os.listdir(test_directory)}")

    node_repo_name = "cardano-node"
    node_repo_dir = test_directory / "cardano_node_dir"

    if node_repo_dir.is_dir():
        repo = gitpython.git_checkout(repository, node_rev)
    else:
        repo = gitpython.git_clone_iohk_repo(node_repo_name, node_repo_dir, node_rev)

    if build_tool == "nix":
        os.chdir(node_repo_dir)
        Path("cardano-node-bin").unlink(missing_ok=True)
        Path("cardano-cli-bin").unlink(missing_ok=True)
        helpers.execute_command("nix build -v .#cardano-node -o cardano-node-bin")
        helpers.execute_command("nix build -v .#cardano-cli -o cardano-cli-bin")
        node.copy_node_executables(node_repo_dir, test_directory, "nix")

    elif build_tool == "cabal":
        cabal_local_file = Path(test_directory) / "sync_tests" / "cabal.project.local"

        cli_rev = "main"
        cli_repo_name = "cardano-cli"
        cli_repo_dir = test_directory / "cardano_cli_dir"

        if cli_repo_dir.is_dir():
            gitpython.git_checkout(repository, cli_rev)
        else:
            gitpython.git_clone_iohk_repo(cli_repo_name, cli_repo_dir, cli_rev)

        # Build cli
        os.chdir(cli_repo_dir)
        shutil.copy2(cabal_local_file, cli_repo_dir)
        print(f" - listdir cli_repo_dir: {os.listdir(cli_repo_dir)}")
        shutil.rmtree("dist-newstyle", ignore_errors=True)
        for line in fileinput.input("cabal.project", inplace=True):
            print(line.replace("tests: True", "tests: False"), end="")
        helpers.execute_command("cabal update")
        helpers.execute_command("cabal build cardano-cli")
        node.copy_node_executables(cli_repo_dir, test_directory, "cabal")
        gitpython.git_checkout(repo, "cabal.project")

        # Build node
        os.chdir(node_repo_dir)
        shutil.copy2(cabal_local_file, node_repo_dir)
        print(f" - listdir node_repo_dir: {os.listdir(node_repo_dir)}")
        shutil.rmtree("dist-newstyle", ignore_errors=True)
        for line in fileinput.input("cabal.project", inplace=True):
            print(line.replace("tests: True", "tests: False"), end="")
        helpers.execute_command("cabal update")
        helpers.execute_command("cabal build cardano-node")
        node.copy_node_executables(node_repo_dir, test_directory, "cabal")
        gitpython.git_checkout(repo, "cabal.project")

    os.chdir(test_directory)
    subprocess.check_call(["chmod", "+x", NODE])
    subprocess.check_call(["chmod", "+x", CLI])
    helpers.print_message(type="info", message="files permissions inside test folder:")
    subprocess.check_call(["ls", "-la"])
    return repo


def main():
    global NODE, CLI, repository
    secs_to_start1, secs_to_start2 = 0, 0
    set_repo_paths()
    set_node_socket_path_env_var()

    print("--- Test data information", flush=True)
    start_test_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    helpers.print_message(type="info", message=f"Test start time: {start_test_time}")
    helpers.print_message(type="warn", message='Test parameters:')
    env = helpers.get_arg_value(args=args, key="environment")
    node_build_mode = helpers.get_arg_value(args=args, key="build_mode")
    node_rev1 =  helpers.get_arg_value(args=args, key="node_rev1")
    node_rev2 =  helpers.get_arg_value(args=args, key="node_rev2")
    tag_no1 =  helpers.get_arg_value(args=args, key="tag_no1")
    tag_no2 =  helpers.get_arg_value(args=args, key="tag_no2")
    node_topology_type1 = helpers.get_arg_value(args=args, key="node_topology1")
    node_topology_type2 = helpers.get_arg_value(args=args, key="node_topology2")
    node_start_arguments1 = helpers.get_arg_value(args=args, key="node_start_arguments1")
    node_start_arguments2 = helpers.get_arg_value(args=args, key="node_start_arguments2")

    repository = None
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

    platform_system, platform_release, platform_version = helpers.get_os_type()
    print(f"- platform: {platform_system, platform_release, platform_version}")

    print("--- Get the cardano-node files", flush=True)
    helpers.print_message(
        type="info",
        message=f"Get the cardano-node and cardano-cli files using - {node_build_mode}",
    )

    start_build_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    if "windows" not in platform_system.lower():
        repository = get_node_files(node_rev1)
    elif "windows" in platform_system.lower():
        repository = get_node_files(node_rev1, build_tool="cabal")
    else:
        helpers.print_message(
            type="error",
            message=f"ERROR: method not implemented yet!!! Only building with NIX is supported at this moment - {node_build_mode}",
        )
        sys.exit(1)

    end_build_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    helpers.print_message(type="info", message=f"  - start_build_time: {start_build_time}")
    helpers.print_message(type="info", message=f"  - end_build_time: {end_build_time}")

    helpers.print_message(type="warn", message="--- node version ")
    cli_version1, cli_git_rev1 = node.get_node_version()

    print(f"  - cardano_cli_version1: {cli_version1}")
    print(f"  - cardano_cli_git_rev1: {cli_git_rev1}")

    print("--- Get the node configuration files")
    rm_node_config_files()
    # TO DO: change the default to P2P when full P2P will be supported on Mainnet
    node.get_node_config_files(env, node_topology_type1)

    print("Enabling the desired cardano node tracers")
    enable_cardano_node_resources_monitoring("config.json")
    enable_cardano_node_tracers("config.json")

    print(f"--- Start node sync test using node_rev1: {node_rev1}")

    helpers.print_message(
        type="ok",
        message="===================================================================================",
    )
    helpers.print_message(
        type="ok",
        message=f"================== Start node sync test using node_rev1: {node_rev1} =============",
    )
    helpers.print_message(
        type="ok",
        message="===================================================================================",
    )
    print()

    start_sync_time1 = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    secs_to_start1 = node.start_node(env=env, node_start_arguments=node_start_arguments1)

    helpers.print_message(type="info", message=" - waiting for the node to sync")
    (
        sync_time_seconds1,
        last_slot_no1,
        latest_chunk_no1,
        era_details_dict1,
        epoch_details_dict1,
    ) = node.wait_for_node_to_sync(env)

    end_sync_time1 = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print(f"secs_to_start1: {secs_to_start1}")
    print(f"start_sync_time1: {start_sync_time1}")
    print(f"end_sync_time1: {end_sync_time1}")
    helpers.print_message(type="warn", message=f"Stop node for: {node_rev1}")
    stop_node(platform_system)
    stop_node(platform_system)

    # we are interested in the node logs only for the main sync - using tag_no1
    test_values_dict = OrderedDict()
    print("--- Parse the node logs and get the relevant data")
    logs_details_dict = get_data_from_logs(NODE_LOG_FILE)
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
        helpers.print_message(
            type="ok",
            message="==============================================================================",
        )
        helpers.print_message(
            type="ok",
            message=f"================= Start sync using node_rev2: {node_rev2} ===================",
        )
        helpers.print_message(
            type="ok",
            message="==============================================================================",
        )

        print("Get the cardano-node and cardano-cli files")
        if "windows" not in platform_system.lower():
            get_node_files(node_rev2, repository)
        elif "windows" in platform_system.lower():
            get_node_files(node_rev2, repository, build_tool="cabal")
        else:
            helpers.print_message(
                type="error",
                message=f"ERROR: method not implemented yet!!! Only building with NIX is supported at this moment - {node_build_mode}",
            )
            sys.exit(1)

        if env == "mainnet" and (node_topology_type1 != node_topology_type2):
            helpers.print_message(type="warn", message="remove the previous topology")
            helpers.delete_file(Path(ROOT_TEST_PATH) / "topology.json")
            print("Getting the node configuration files")
            node.get_node_config_files(env, node_topology_type2)

        helpers.print_message(type="warn", message="node version")
        cli_version2, cli_git_rev2 = node.get_node_version()
        print(f" - cardano_cli_version2: {cli_version2}")
        print(f" - cardano_cli_git_rev2: {cli_git_rev2}")
        print()
        print(f"================ Start node using node_rev2: {node_rev2} ====================")
        start_sync_time2 = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        secs_to_start2 = node.start_node(env=env, node_start_arguments=node_start_arguments2)


        helpers.print_message(
            type="info",
            message=f" - waiting for the node to sync - using node_rev2: {node_rev2}",
        )
        (
            sync_time_seconds2,
            last_slot_no2,
            latest_chunk_no2,
            era_details_dict2,
            epoch_details_dict2,
        ) = node.wait_for_node_to_sync(env)
        end_sync_time2 = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        helpers.print_message(type="warn", message=f"Stop node for: {node_rev2}")
        stop_node(platform_system)
        stop_node(platform_system)

    chain_size = helpers.get_directory_size(Path(ROOT_TEST_PATH) / "db")

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
    epoch_details = OrderedDict()
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
    test_values_dict["sync_time1"] = str(timedelta(seconds=int(sync_time_seconds1)))
    test_values_dict["sync_time_seconds2"] = sync_time_seconds2
    test_values_dict["sync_time2"] = str(timedelta(seconds=int(sync_time_seconds2)))
    test_values_dict["total_chunks1"] = latest_chunk_no1
    test_values_dict["total_chunks2"] = latest_chunk_no2
    test_values_dict["platform_system"] = platform_system
    test_values_dict["platform_release"] = platform_release
    test_values_dict["platform_version"] = platform_version
    test_values_dict["chain_size_bytes"] = chain_size
    test_values_dict["sync_duration_per_epoch"] = json.dumps(epoch_details)
    test_values_dict["eras_in_test"] = json.dumps(list(era_details_dict1.keys()))
    test_values_dict["no_of_cpu_cores"] = os.cpu_count()
    test_values_dict["total_ram_in_GB"] = helpers.get_total_ram_in_GB()
    test_values_dict["epoch_no_d_zero"] = get_epoch_no_d_zero()
    test_values_dict["start_slot_no_d_zero"] = get_start_slot_no_d_zero()
    test_values_dict["hydra_eval_no1"] = node_rev1
    test_values_dict["hydra_eval_no2"] = node_rev2

    print("--- Write tests results to file")
    os.chdir(Path(ROOT_TEST_PATH))
    current_directory = Path.cwd()
    print(f"current_directory: {current_directory}")
    print(f"Write the test values to the {current_directory / RESULTS_FILE_NAME} file")
    with open(RESULTS_FILE_NAME, "w") as results_file:
        json.dump(test_values_dict, results_file, indent=2)

    print("--- Copy the node logs")
    # sometimes uploading the artifacts fails because the node still writes into
    # the log file during the upload even though an attepmt to stop it was made
    copy_log_file_artifact(NODE_LOG_FILE, NODE_LOG_FILE_ARTIFACT)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Cardano Node sync test\n\n")

    parser.add_argument(
        "-b", "--build_mode", help="how to get the node files - nix, cabal, prebuilt"
    )
    parser.add_argument(
        "-e",
        "--environment",
        help="the environment on which to run the sync test - shelley-qa, preview, preprod, mainnet",
    )
    parser.add_argument(
        "-r1",
        "--node_rev1",
        help="desired cardano-node revision - cardano-node tag or branch (used for initial sync, from clean state)",
    )
    parser.add_argument(
        "-r2",
        "--node_rev2",
        help="desired cardano-node revision - cardano-node tag or branch (used for final sync, from existing state)",
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
        help="type of node topology used for the initial sync - legacy, non-bootstrap-peers, bootstrap-peers",
    )
    parser.add_argument(
        "-n2",
        "--node_topology2",
        help="type of node topology used for final sync (after restart) - legacy, non-bootstrap-peers, bootstrap-peers",
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
