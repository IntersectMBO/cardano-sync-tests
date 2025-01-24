import collections
import datetime
import json
import logging
import os
import platform
import shutil
import subprocess
import time
import typing as tp
import urllib.request
from collections import OrderedDict

from pathlib import Path
from typing import Any

from sync_tests.utils import blockfrost
from sync_tests.utils import db_sync
from sync_tests.utils import explorer
from sync_tests.utils import helpers

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ROOT_TEST_PATH = Path.cwd()
ENVIRONMENT = os.getenv("environment")
NODE_LOG_FILE = ROOT_TEST_PATH / f"cardano-node/node_{ENVIRONMENT}_logfile.log"


def get_testnet_value(env: str) -> str:
    """Returns the appropriate testnet magic value for the specified environment."""
    if env == "mainnet":
        magic_value = "--mainnet"
    elif env == "testnet":
        magic_value = "--testnet-magic 1097911063"
    elif env == "staging":
        magic_value = "--testnet-magic 633343913"
    elif env == "shelley-qa":
        magic_value = "--testnet-magic 3"
    elif env == "preview":
        magic_value = "--testnet-magic 2"
    elif env == "preprod":
        magic_value = "--testnet-magic 1"
    else:
        raise ValueError(
            f"Invalid environment: {env}. "
            f"Must be one of: 'mainnet', 'testnet', 'staging', 'shelley-qa', "
            f"'preview', or 'preprod'."
        )
    
    return magic_value
    

def calculate_current_slot(env: str) -> int:
    """Calculate the current slot number based on the environment's predefined."""
    current_time = datetime.datetime.now(datetime.UTC)

    start_times = {
        "testnet": {"byron": "2019-07-24T20:20:16", "shelley": "2020-07-28T20:20:16"},
        "staging": {"byron": "2017-09-26T18:23:33", "shelley": "2020-08-01T18:23:33"},
        "mainnet": {"byron": "2017-09-23T21:44:51", "shelley": "2020-07-29T21:44:51"},
        "shelley-qa": {"byron": "2020-08-17T13:00:00", "shelley": "2020-08-17T17:00:00"},
        "preprod": {"byron": "2022-06-01T00:00:00", "shelley": "2022-06-21T00:00:00"},
        "preview": {"byron": "2022-08-09T00:00:00", "shelley": "2022-08-09T00:00:00"}
    }

    byron_start_time = helpers.convert_to_datetime(start_times[env]["byron"])
    shelley_start_time = helpers.convert_to_datetime(start_times[env]["shelley"])

    last_slot_no = int((shelley_start_time - byron_start_time).total_seconds() / 20 +
                       (current_time - shelley_start_time).total_seconds())

    return last_slot_no


def get_no_of_slots_in_era(env: str, era_name: str, no_of_epochs_in_era: int) -> int:
    """Calculate the total number of slots in an era."""
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


def get_current_tip(env:str) -> dict:
    """Retrieves the current tip of the Cardano node."""
    os.chdir(ROOT_TEST_PATH / "cardano-node")
    cmd = "./_cardano-cli latest query tip " + get_testnet_value(env)

    tip_info = {}

    for i in range(20):
        try:
            output = (
                subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
                .decode("utf-8")
                .strip()
            )
            output_json = json.loads(output)
            tip_info["epoch"] = int(output_json.get("epoch", 0))
            tip_info["block"] = int(output_json.get("block", 0))
            tip_info["hash_value"] = output_json.get("hash", "")
            tip_info["slot"] = int(output_json.get("slot", 0))
            tip_info["era"] = output_json.get("era", "").lower()
            tip_info["sync_progress"] = (
                int(float(output_json.get("syncProgress", 0.0)))
                if "syncProgress" in output_json
                else None
            )

            break  # Exit the loop once the tip_info is successfully populated
        except subprocess.CalledProcessError as e:
            logging.error(f" === Waiting 60s before retrying to get the tip again - {i}")
            logging.error(f" !!!ERROR: command {e.cmd} return with error (code {e.returncode}): "
                          f"{' '.join(str(e.output).split())}")
            if "Invalid argument" in str(e.output):
                db_sync.emergency_upload_artifacts(env)
                exit(1)
            pass
        time.sleep(60)
    else:
        # If the loop completes without success, upload artifacts and exit
        db_sync.emergency_upload_artifacts(env)
        exit(1)

    return tip_info


def wait_for_node_to_start(env: str) -> int:
    """Waits for Cardano node to start."""
    # When starting from clean state it might take ~30 secs for the cli to work.
    # When starting from existing state it might take >10 mins for the cli to work (opening db and
    # replaying the ledger).
    start_counter = time.perf_counter()
    get_current_tip(env=env)
    stop_counter = time.perf_counter()

    start_time_seconds = int(stop_counter - start_counter)
    logging.info(f" === It took {start_time_seconds} seconds for the QUERY TIP command to be available")
    return start_time_seconds


def start_node(env: str, node_start_arguments: tp.Optional[tp.List[str]] = None) -> int:
    """Starts the Cardano node in the current working directory."""
    os.chdir(ROOT_TEST_PATH / "cardano-node")
    current_directory = os.getcwd()
    start_args = ' '.join(node_start_arguments) if node_start_arguments else ""

    logging.info(f"current_directory: {current_directory}")

    if platform.system().lower() == 'windows':
        cmd = (
            f"./_cardano-node run --topology {env}-topology.json "
            f"--database-path db "
            f"--host-addr 0.0.0.0 "
            f"--port 3000 "
            f"--socket-path \\\\.\pipe\cardano-node "
            f"--config config.json {start_args}"
        ).strip()
    else:
        cmd = (
            f"./_cardano-node run --topology {env}-topology.json --database-path "
            f"{Path(ROOT_TEST_PATH) / 'cardano-node' / 'db'} "
            f"--host-addr 0.0.0.0 --port 3000 --config "
            f"{env}-config.json --socket-path ./db/node.socket"
        ).strip()

    logfile = open(NODE_LOG_FILE, "w+")
    logging.info(f"start node cmd: {cmd}")

    try:
        subprocess.Popen(cmd.split(" "), stdout=logfile, stderr=logfile)
        logging.info("Waiting for db folder to be created")
        counter = 0
        timeout_counter = 60
        node_db_dir = current_directory + "/db"
        while not os.path.isdir(node_db_dir):
            time.sleep(1)
            counter += 1
            if counter > timeout_counter:
                logging.error(
                    f"ERROR: waited {timeout_counter} seconds and the DB folder was not created yet")
                exit(1)

        logging.info(f"DB folder was created after {counter} seconds")
        secs_to_start = wait_for_node_to_start(env)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )
    return secs_to_start


def get_cli_version() -> tp.Tuple[str, str]:
    """Get cardano-cli version"""
    try:
        cmd = "./_cardano-cli --version"
        output = (
            subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
                .decode("utf-8")
                .strip()
        )
        cardano_cli_version = output.split("git rev ")[0].strip()
        cardano_cli_git_rev = output.split("git rev ")[1].strip()
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )

    return str(cardano_cli_version), str(cardano_cli_git_rev)


def wait_for_node_to_sync(
        env: str, sync_percentage: float = 99.9
) -> tuple[int, int, str, OrderedDict, OrderedDict]:
    """Waits for the Cardano node to sync."""
    era_details_dict = collections.OrderedDict()
    epoch_details_dict = collections.OrderedDict()

    start_sync = time.perf_counter()
    tip_info = get_current_tip(env=env)
    node_sync_progress = tip_info["sync_progress"]
    log_frequency = 20 if env == "mainnet" else 3

    logging.info("--- Waiting for Node to sync")
    logging.info(f"node progress [%]: {node_sync_progress}")
    counter = 0

    while node_sync_progress < sync_percentage:
        if counter % log_frequency == 0:
            logging.info(
                f"node progress [%]: {tip_info['sync_progress']}, "
                f"epoch: {tip_info['epoch']}, "
                f"block: {tip_info['block']}, "
                f"slot: {tip_info['slot']}, "
                f"era: {tip_info['era']}"
            )

        epoch = tip_info["epoch"]
        era = tip_info["era"]
        start_sync_time = datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        if era not in era_details_dict:
            if env == 'mainnet':
                start_time = blockfrost.get_epoch_start_datetime(epoch)
            else:
                start_time = explorer.get_epoch_start_datetime(env, epoch)

            era_details_dict[era] = {
                'start_epoch': epoch,
                'start_time': start_time,
                'start_sync_time': start_sync_time
            }

        if tip_info["epoch"] not in epoch_details_dict:
            epoch_details_dict[tip_info["epoch"]] = {
                'start_sync_time': start_sync_time
            }

        time.sleep(60)
        counter += 1
        tip_info = get_current_tip(env=env)

    os.chdir(Path(ROOT_TEST_PATH) / 'db' / 'immutable')
    chunk_files = sorted(os.listdir(os.getcwd()), key=os.path.getmtime)
    latest_chunk_no = chunk_files[-1].split(".")[0]
    os.chdir(Path(ROOT_TEST_PATH))
    logging.info(f"Sync done!; latest_chunk_no: {latest_chunk_no}")

    end_sync = time.perf_counter()
    sync_duration = int(end_sync - start_sync)
    last_slot_no = calculate_current_slot(env)

    end_sync_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # add "end_sync_time", "slots_in_era", "sync_duration_secs" and "sync_speed_sps" for each era;
    # for the last/current era, "end_sync_time" = current_utc_time / end_of_sync_time
    eras_list = list(era_details_dict.keys())
    for era in eras_list:
        next_era_details = era_details_dict.get(eras_list[eras_list.index(era) + 1], {})
        next_era_start_time = next_era_details["start_sync_time"] if next_era_details else None
        end_sync_time = next_era_start_time or end_sync_time
        last_epoch = int(next_era_start_time) - 1 if next_era_start_time else era_details_dict[era]["epoch"]

        era_details_dict[era].update({
            'last_epoch': last_epoch,
            'end_sync_time': end_sync_time,
            'slots_in_era': get_no_of_slots_in_era(env, era, last_epoch - era_details_dict[era]['start_epoch'] + 1),
            'sync_duration_secs': int(
                (
                        helpers.convert_to_datetime(end_sync_time) -
                        helpers.convert_to_datetime(era_details_dict[era]['start_sync_time'])
                ).total_seconds()
            ),
            'sync_speed_sps': era_details_dict[era]['slots_in_era'] // era_details_dict[era]['sync_duration_secs']
        })

    # calculate and add "end_sync_time" and "sync_duration_secs" for each epoch;
    epoch_list = list(epoch_details_dict.keys())
    for epoch in epoch_list:
        next_epoch_sync_time = epoch_details_dict.get(epoch_list[epoch_list.index(epoch) + 1], {}).get('start_sync_time')
        end_sync_time = next_epoch_sync_time or end_sync_time

        epoch_details_dict[epoch].update({
            'end_sync_time': end_sync_time,
            'sync_duration_secs': int(
                (
                        helpers.convert_to_datetime(end_sync_time) -
                        helpers.convert_to_datetime(epoch_details_dict[epoch]['start_sync_time'])
                ).total_seconds()
            ),
        })

    return sync_duration, last_slot_no, latest_chunk_no, era_details_dict, epoch_details_dict


def copy_node_executables(build_method: str = "nix") -> None:
    """Copies the Cardano node executables built with the specified build method."""
    current_directory = os.getcwd()
    os.chdir(ROOT_TEST_PATH)
    node_dir = Path.cwd() / "cardano-node"
    os.chdir(node_dir)
    logging.info(f"current_directory: {os.getcwd()}")

    result = subprocess.run(["nix", "--version"], stdout=subprocess.PIPE, text=True, check=True)
    logging.info(f"Nix version: {result.stdout.strip()}")

    try:
        if build_method == "nix":
            node_binary_location = "cardano-node-bin/bin/cardano-node"
            node_cli_binary_location = "cardano-cli-bin/bin/cardano-cli"
            shutil.copy2(node_binary_location, "_cardano-node")
            shutil.copy2(node_cli_binary_location, "_cardano-cli")
            os.chdir(current_directory)
        # Path for copying binaries built with cabal
        else:
            find_node_cmd = ["find", ".", "-name", "cardano-node", "-executable", "-type", "f"]
            output_find_node_cmd = (
                subprocess.check_output(find_node_cmd, stderr=subprocess.STDOUT, timeout=15)
                .decode("utf-8")
                .strip()
            )
            logging.info(f"Find cardano-node output: {output_find_node_cmd}")
            shutil.copy2(output_find_node_cmd, "_cardano-node")

            find_cli_cmd = ["find", ".", "-name", "cardano-cli", "-executable", "-type", "f"]
            output_find_cli_cmd = (
                subprocess.check_output(find_cli_cmd, stderr=subprocess.STDOUT, timeout=15)
                .decode("utf-8")
                .strip()
            )
            logging.info(f"Find cardano-cli output: {output_find_cli_cmd}")
            shutil.copy2(output_find_cli_cmd, "_cardano-cli")
            os.chdir(current_directory)
    except Exception as e:
        raise RuntimeError(f"!!! ERROR - could not copy the Cardano node executables: {e}")


def get_node_config_files(env: str, node_topology_type: tp.Optional[str] = "") -> None:
    """Downloads Cardano node configuration files for the specified environment."""
    base_url = "https://book.play.dev.cardano.org/environments/"
    filenames = [
        (base_url + env + "/config.json", f"{env}-config.json"),
        (base_url + env + "/byron-genesis.json", "byron-genesis.json"),
        (base_url + env + "/shelley-genesis.json", "shelley-genesis.json"),
        (base_url + env + "/alonzo-genesis.json", "alonzo-genesis.json"),
        (base_url + env + "/conway-genesis.json", "conway-genesis.json"),
        (base_url + env + "/topology.json", f"{env}-topology.json")
    ]
    
    if env == "mainnet" and node_topology_type == "non-bootstrap-peers":
        filenames.append((base_url + env + "/topology-non-bootstrap-peers.json", f"{env}-topology.json"))
    elif env == "mainnet" and node_topology_type == "legacy":
        filenames.append((base_url + env + "/topology-legacy.json", f"{env}-topology.json"))
        disable_p2p_node_config()
    else:
        filenames.append((base_url + env + "/topology.json", f"{env}-topology.json"))
        
    for url, filename in filenames:
        try:
            urllib.request.urlretrieve(url, filename)
            # Check if the file exists after download
            if not os.path.isfile(filename):
                raise FileNotFoundError(f"Downloaded file '{filename}' does not exist.")
        except Exception as e:
            logging.exception(f"Error downloading {url}: {e}")
            exit(1)


def disable_p2p_node_config() -> None:
    """Disables P2P-related configuration in the "config.json" file."""
    with open("config.json", 'r') as file:
        config = json.load(file)

    config["EnableP2P"] = False
    config["PeerSharing"] = False

    # Write the updated configuration back to the JSON file
    with open("config.json", "w") as new_file:
        json.dump(config, new_file, indent=4)

    logging.info("Configuration updated successfully.")
