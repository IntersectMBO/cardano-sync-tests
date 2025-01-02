import hashlib
import logging
import os
import platform
import shutil
import mmap
import subprocess
import requests
import urllib.request
import tarfile
import xmltodict
import json
import shlex
import psycopg2
from assertpy import assert_that

from os.path import normpath, basename
from pathlib import Path
from psutil import process_iter

import time

import sync_tests.utils.utils as utils

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ONE_MINUTE = 60
ROOT_TEST_PATH = Path.cwd()

# Environment Variables
ENVIRONMENT = os.getenv('environment')
NODE_PR = os.getenv('node_pr')
NODE_BRANCH = os.getenv('node_branch')
NODE_VERSION = os.getenv('node_version')
DB_SYNC_BRANCH = os.getenv('db_sync_branch')
DB_SYNC_VERSION = os.getenv('db_sync_version')

# System Information
POSTGRES_DIR = ROOT_TEST_PATH.parents[0]
POSTGRES_USER = subprocess.run(['whoami'], stdout=subprocess.PIPE).stdout.decode('utf-8').strip()

# Log and Stats Paths
db_sync_perf_stats = []
DB_SYNC_PERF_STATS_FILE = ROOT_TEST_PATH / f"cardano-db-sync/db_sync_{ENVIRONMENT}_performance_stats.json"
NODE_LOG_FILE = ROOT_TEST_PATH / f"cardano-node/node_{ENVIRONMENT}_logfile.log"
DB_SYNC_LOG_FILE = ROOT_TEST_PATH / f"cardano-db-sync/db_sync_{ENVIRONMENT}_logfile.log"
EPOCH_SYNC_TIMES_FILE = ROOT_TEST_PATH / f"cardano-db-sync/epoch_sync_times_{ENVIRONMENT}_dump.json"

# Archive Names
NODE_ARCHIVE_NAME = f"cardano_node_{ENVIRONMENT}_logs.zip"
DB_SYNC_ARCHIVE_NAME = f"cardano_db_sync_{ENVIRONMENT}_logs.zip"
SYNC_DATA_ARCHIVE_NAME = f"epoch_sync_times_{ENVIRONMENT}_dump.zip"
PERF_STATS_ARCHIVE_NAME = f"db_sync_{ENVIRONMENT}_perf_stats.zip"


class sh_colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def print_color_log(log_type, message):
    print(f"{log_type}{message}{sh_colors.ENDC}")


def get_machine_name():
    return platform.node()


def export_env_var(name, value):
    os.environ[name] = str(value)


def wait(seconds):
    time.sleep(seconds)


def make_tarfile(output_filename, source_dir):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))

       
def upload_artifact(file):
    path=f"{ENVIRONMENT}/{DB_SYNC_BRANCH}/"  
    cmd = ['which', 'buildkite-agent']
    p = subprocess.run(cmd, stdout=subprocess.PIPE).stdout.decode('utf-8').strip()

    if ('buildkite-agent' in p):
        _upload_buildkite_artifact(file)
        return
    if (p == ''):
        _upload_to_S3_bucket(file, path)


def _upload_to_S3_bucket(file, path, expected_file_size_limit_in_mb=20):
    file_size_in_mb = get_file_size(file)    
    this_machine = get_machine_name()
    slow_machines = [ 'workstation', 'actina' ]
    
    if this_machine in slow_machines and file_size_in_mb > expected_file_size_limit_in_mb:
        logging.info(f"This machine has very slow network upload speed - skipping file {file} upload.")
        logging.info(f"File has {file_size_in_mb} [MB]. Max file size limit for upload is set to {expected_file_size_limit_in_mb} [MB]")
        return
    
    try:
        cmd = ["aws", "s3", "cp", f"{file}", f"s3://cardano-qa-bucket/{path}"]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding='utf-8')
        outs, errs = p.communicate(timeout=1200)
        if errs:
            logging.error(f"Error occured during {file} upload to S3: {errs}")
        if outs is not None: print(f"Output from {file} upload to S3: {outs}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' returned with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )
    except subprocess.TimeoutExpired as e:
        p.kill()
        logging.error(f"TimeoutExpired exception occured during {file} upload to S3: {e}")
        

def _upload_buildkite_artifact(file):
    try:
        cmd = ["buildkite-agent", "artifact", "upload", f"{file}"]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding='utf-8')
        outs, errs = p.communicate(timeout=1200)
        if errs:
            logging.error(f"Error occured during {file} upload to BuildKite: {errs}")
            p.kill()
        if outs is not None: print(f"Output from {file} upload to BuildKite: {outs}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' returned with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )
    except subprocess.TimeoutExpired as e:
        p.kill()
        logging.error(f"TimeoutExpired exception occured during {file} upload to BuildKite: {e}")
              
              
def create_node_database_archive(env):
    current_directory = os.getcwd()
    os.chdir(ROOT_TEST_PATH)
    os.chdir(Path.cwd() / 'cardano-node')
    node_directory = os.getcwd()
    node_db_archive = f"node-db-{env}.tar.gz"
    make_tarfile(node_db_archive, "db")
    os.chdir(current_directory)
    node_db_archive_path = node_directory + f"/{node_db_archive}"
    return node_db_archive_path

        
def set_buildkite_meta_data(key, value):
    p = subprocess.Popen(["buildkite-agent", "meta-data", "set", f"{key}", f"{value}"])
    outs, errs = p.communicate(timeout=15)


def get_buildkite_meta_data(key):
    p = subprocess.Popen(["buildkite-agent", "meta-data", "get", f"{key}"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    outs, errs = p.communicate(timeout=15)
    return outs.decode("utf-8").strip()


def write_data_as_json_to_file(file, data):
    with open(file, 'w') as test_results_file:
        json.dump(data, test_results_file, indent=2)


def print_file(file, number_of_lines = 0):
    with open(file) as f:
        contents = f.read()
    if number_of_lines:
        for index, line in enumerate(contents.split(os.linesep)):
            if index < number_of_lines + 1:
                logging.info(line)
            else: break
    else:
        logging.info(contents)
        return contents


def get_process_info(proc_name):
    for proc in process_iter():
        if proc_name in proc.name():
            return proc


def stop_process(proc_name):
    for proc in process_iter():
        if proc_name in proc.name():
            logging.info(f" --- Terminating the {proc_name} process - {proc}")
            proc.terminate()
    time.sleep(30)
    for proc in process_iter():
        if proc_name in proc.name():
            logging.info(f" !!! ERROR: {proc_name} process is still active. Killing forcefully - {proc}")
            proc.kill()


def create_dir(dir_name, root='.'):
    Path(f"{root}/{dir_name}").mkdir(parents=True, exist_ok=True)
    return f"{root}/{dir_name}"


def remove_dir(dir_name):
    try:
        shutil.rmtree(dir_name)
    except OSError as e:
        logging.error("Error: %s : %s" % (dir_name, e.strerror))


def get_file_sha_256_sum(filename):
    with open(filename,"rb") as f:
        bytes = f.read()
        readable_hash = hashlib.sha256(bytes).hexdigest();
        return readable_hash

        
def print_n_last_lines_from_file(n, file_name):
    logs = subprocess.run(['tail', "-n", f"{n}", f"{file_name}"], stdout=subprocess.PIPE).stdout.decode('utf-8').strip().rstrip().splitlines()
    for line in logs:
        logging.info(line)

    
def execute_command(command):
    logging.info(f"--- Execute command {command}")
    try:
        cmd = shlex.split(command)
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        outs, errors = process.communicate(timeout=3600)               
        if errors:
            logging.info(f"Warnings or Errors: {errors}")
        logging.info(f"Output of command: {command} : {outs}")
        exit_code = process.returncode
        if (exit_code != 0):
            logging.error(f"Command {command} returned exit code: {exit_code}")
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        logging.error(f"Command {command} returned exception: {e}")
        raise
    
  
def get_last_perf_stats_point():
    try:
        last_perf_stats_point = db_sync_perf_stats[-1]
    except Exception as e:
        logging.exception(f"Exception in get_last_perf_stats_point: {e}")
        stats_data_point = {"time": 0, "slot_no": 0, "cpu_percent_usage": 0, "rss_mem_usage": 0}
        db_sync_perf_stats.append(stats_data_point)
        last_perf_stats_point = db_sync_perf_stats[-1]

    return last_perf_stats_point


def get_testnet_value(env):
    if env == "mainnet":
        return "--mainnet"
    if env == "preprod":
        return "--testnet-magic 1"
    if env == "preview":
        return "--testnet-magic 2"
    elif env == "shelley-qa":
        return "--testnet-magic 3"
    elif env == "staging":
        return "--testnet-magic 633343913"
    else:
        return None


def get_log_output_frequency(env):
    if env == "mainnet":
        return 20
    else:
        return 3


def export_epoch_sync_times_from_db(env, file, snapshot_epoch_no = 0):
    os.chdir(ROOT_TEST_PATH / "cardano-db-sync")
    try:
        p = subprocess.Popen(["psql", f"{env}", "-t", "-c", f"\o {file}", "-c", f"SELECT array_to_json(array_agg(epoch_sync_time), FALSE) FROM epoch_sync_time where no >= {snapshot_epoch_no};" ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = (p.decode("utf-8").strip() for p in p.communicate(timeout=600))
        if err:
            logging.error(f"Error during exporting epoch sync times from db: {err}. Killing extraction process.")
            p.kill()
        return out
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError) as e:
        p.kill()
        logging.error(f"Error during exporting epoch sync times from db: {e}. Killing extraction process.")
    except Exception as e:
        logging.exception(f"Error during exporting epoch sync times from db: {e}. Killing extraction process.")
        p.kill()


def emergency_upload_artifacts(env):
    write_data_as_json_to_file(DB_SYNC_PERF_STATS_FILE, db_sync_perf_stats)
    export_epoch_sync_times_from_db(env, EPOCH_SYNC_TIMES_FILE)

    utils.zip_file(PERF_STATS_ARCHIVE_NAME, DB_SYNC_PERF_STATS_FILE)
    utils.zip_file(SYNC_DATA_ARCHIVE_NAME, EPOCH_SYNC_TIMES_FILE)
    utils.zip_file(DB_SYNC_ARCHIVE_NAME, DB_SYNC_LOG_FILE)
    utils.zip_file(NODE_ARCHIVE_NAME, NODE_LOG_FILE)

    upload_artifact(PERF_STATS_ARCHIVE_NAME)
    upload_artifact(SYNC_DATA_ARCHIVE_NAME)
    upload_artifact(DB_SYNC_ARCHIVE_NAME)
    upload_artifact(NODE_ARCHIVE_NAME)

    stop_process('cardano-db-sync')
    stop_process('cardano-node')


def get_node_config_files(env):
    base_url = "https://book.play.dev.cardano.org/environments/"
    filenames = [
        (base_url + env + "/config.json", f"{env}-config.json"),
        (base_url + env + "/byron-genesis.json", "byron-genesis.json"),
        (base_url + env + "/shelley-genesis.json", "shelley-genesis.json"),
        (base_url + env + "/alonzo-genesis.json", "alonzo-genesis.json"),
        (base_url + env + "/conway-genesis.json", "conway-genesis.json"),
        (base_url + env + "/topology.json", f"{env}-topology.json")
    ]
    for url, filename in filenames:
        try:
            urllib.request.urlretrieve(url, filename)
            # Check if the file exists after download
            if not os.path.isfile(filename):
                raise FileNotFoundError(f"Downloaded file '{filename}' does not exist.")
        except Exception as e:
            logging.exception(f"Error downloading {url}: {e}")
            exit(1)


def copy_node_executables(build_method="nix"):
    current_directory = os.getcwd()
    os.chdir(ROOT_TEST_PATH)
    node_dir = Path.cwd() / 'cardano-node'
    node_bin_dir = node_dir / "cardano-node-bin/"
    os.chdir(node_dir)
    logging.info(f"current_directory: {os.getcwd()}")

    result = subprocess.run(['nix', '--version'], stdout=subprocess.PIPE, text=True, check=True)
    logging.info(f"Nix version: {result.stdout.strip()}")
    
    if build_method == "nix":
        node_binary_location = "cardano-node-bin/bin/cardano-node"
        node_cli_binary_location = "cardano-cli-bin/bin/cardano-cli"
        shutil.copy2(node_binary_location, "_cardano-node")
        shutil.copy2(node_cli_binary_location, "_cardano-cli")
        os.chdir(current_directory)
        return
    
    # Path for copying binaries built with cabal
    try:
        find_node_cmd = [ "find", ".", "-name", "cardano-node", "-executable", "-type", "f" ]
        output_find_node_cmd = (
            subprocess.check_output(find_node_cmd, stderr=subprocess.STDOUT, timeout=15)
            .decode("utf-8")
            .strip()
        )
        logging.info(f"Find cardano-node output: {output_find_node_cmd}")
        shutil.copy2(output_find_node_cmd, "_cardano-node")

        find_cli_cmd = [ "find", ".", "-name", "cardano-cli", "-executable", "-type", "f" ]
        output_find_cli_cmd = (
            subprocess.check_output(find_cli_cmd, stderr=subprocess.STDOUT, timeout=15)
            .decode("utf-8")
            .strip()
        )
        logging.info(f"Find cardano-cli output: {output_find_cli_cmd}")
        shutil.copy2(output_find_cli_cmd, "_cardano-cli")       
        os.chdir(current_directory)

    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )


def get_node_version():
    current_directory = os.getcwd()
    os.chdir(ROOT_TEST_PATH / "cardano-node")
    try:
        cmd = "./_cardano-cli --version"
        output = (
            subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
                .decode("utf-8")
                .strip()
        )
        cardano_cli_version = output.split("git rev ")[0].strip()
        cardano_cli_git_rev = output.split("git rev ")[1].strip()
        os.chdir(current_directory)
        return str(cardano_cli_version), str(cardano_cli_git_rev)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )


def download_and_extract_node_snapshot(env):
    current_directory = os.getcwd()
    headers = {'User-Agent': 'Mozilla/5.0'}
    if env == "mainnet":
        snapshot_url = 'https://update-cardano-mainnet.iohk.io/cardano-node-state/db-mainnet.tar.gz'
    else:
        snapshot_url = '' # no other environments are supported for now

    archive_name = f"db-{env}.tar.gz"
    
    logging.info("Download node snapshot file:")
    logging.info(f" - current_directory: {current_directory}")
    logging.info(f" - download_url: {snapshot_url}")
    logging.info(f" - archive name: {archive_name}")

    with requests.get(snapshot_url, headers = headers, stream = True, timeout = 2800) as r:
        r.raise_for_status()
        with open(archive_name, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    logging.info(f" ------ listdir (before archive extraction): {os.listdir(current_directory)}")
    tf = tarfile.open(Path(current_directory) / archive_name)
    tf.extractall(Path(current_directory))
    os.rename(f"db-{env}","db")
    utils.delete_file(Path(current_directory) / archive_name)
    logging.info(f" ------ listdir (after archive extraction): {os.listdir(current_directory)}")


def set_node_socket_path_env_var_in_cwd():    
    os.chdir(ROOT_TEST_PATH / "cardano-node")
    current_directory = os.getcwd()
    if not 'cardano-node' == basename(normpath(current_directory)):
        raise Exception(f"You're not inside 'cardano-node' directory but in: {current_directory}")
    socket_path = 'db/node.socket'
    export_env_var("CARDANO_NODE_SOCKET_PATH", socket_path)


def get_node_tip(env, timeout_minutes=20):
    current_directory = os.getcwd()
    os.chdir(ROOT_TEST_PATH / "cardano-node")
    cmd = "./_cardano-cli latest query tip " + get_testnet_value(env)

    for i in range(timeout_minutes):
        try:
            output = (
                subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT).decode("utf-8").strip()
            )
            os.chdir(current_directory)
            output_json = json.loads(output)            
            if output_json["epoch"] is not None:
                output_json["epoch"] = int(output_json["epoch"])
            if "block" not in output_json:
                output_json["block"] = None
            else:
                output_json["block"] = int(output_json["block"])
            if "hash" not in output_json:
                output_json["hash"] = None
            if "slot" not in output_json:
                output_json["slot"] = None
            else:
                output_json["slot"] = int(output_json["slot"])
            if "syncProgress" not in output_json:
                output_json["syncProgress"] = None
            else:
                output_json["syncProgress"] = float(output_json["syncProgress"])

            return output_json["epoch"], output_json["block"], output_json["hash"], \
                   output_json["slot"], output_json["era"].lower(), output_json["syncProgress"]
        except subprocess.CalledProcessError as e:
            logging.error(f" === Waiting 60s before retrying to get the tip again - {i}")
            logging.error(f"     !!!ERROR: command {e.cmd} return with error (code {e.returncode}): {' '.join(str(e.output).split())}")
            if "Invalid argument" in str(e.output):
                emergency_upload_artifacts(env)
                exit(1)
            pass
        time.sleep(ONE_MINUTE)
    emergency_upload_artifacts(env)
    exit(1)


def wait_for_node_to_start(env):
    # when starting from clean state it might take ~30 secs for the cli to work
    # when starting from existing state it might take >10 mins for the cli to work (opening db and
    # replaying the ledger)
    start_counter = time.perf_counter()
    get_node_tip(env)
    stop_counter = time.perf_counter()

    start_time_seconds = int(stop_counter - start_counter)
    logging.info(f" === It took {start_time_seconds} seconds for the QUERY TIP command to be available")
    return start_time_seconds


def wait_for_node_to_sync(env, sync_percentage = 99.9):
    start_sync = time.perf_counter()
    *data, node_sync_progress = get_node_tip(env)
    log_frequency = get_log_output_frequency(env)
    logging.info("--- Waiting for Node to sync")
    logging.info(f"node progress [%]: {node_sync_progress}")
    counter = 0

    while node_sync_progress < sync_percentage:
        if counter % log_frequency == 0:
            node_epoch_no, node_block_no, node_hash, node_slot, node_era, node_sync_progress = get_node_tip(env)
            logging.info(f"node progress [%]: {node_sync_progress}, epoch: {node_epoch_no}, block: {node_block_no}, slot: {node_slot}, era: {node_era}")
        *data, node_sync_progress = get_node_tip(env)
        time.sleep(ONE_MINUTE)
        counter += 1

    end_sync = time.perf_counter()
    sync_time_seconds = int(end_sync - start_sync)
    return sync_time_seconds

    
def start_node_in_cwd(env):
    os.chdir(ROOT_TEST_PATH / "cardano-node")
    current_directory = os.getcwd()
    if not 'cardano-node' == basename(normpath(current_directory)):
        raise Exception(f"You're not inside 'cardano-node' directory but in: {current_directory}")
       
    logging.info(f"current_directory: {current_directory}")
    cmd = (
        f"./_cardano-node run --topology {env}-topology.json --database-path "
        f"{Path(ROOT_TEST_PATH) / 'cardano-node' / 'db'} "
        f"--host-addr 0.0.0.0 --port 3000 --config "
        f"{env}-config.json --socket-path ./db/node.socket"
    )

    logfile = open(NODE_LOG_FILE, "w+")
    logging.info(f"start node cmd: {cmd}")

    try:
        p = subprocess.Popen(cmd.split(" "), stdout=logfile, stderr=logfile)
        logging.info("waiting for db folder to be created")
        counter = 0
        timeout_counter = 1 * ONE_MINUTE
        node_db_dir = current_directory + "/db"
        while not os.path.isdir(node_db_dir):
            time.sleep(1)
            counter += 1
            if counter > timeout_counter:
                logging.error(
                    f"ERROR: waited {timeout_counter} seconds and the DB folder was not created yet")
                node_startup_error = print_file(NODE_LOG_FILE)
                print_color_log(sh_colors.FAIL, f"Error: {node_startup_error}")
                exit(1)

        logging.info(f"DB folder was created after {counter} seconds")
        secs_to_start = wait_for_node_to_start(env)
        logging.info(f" - listdir current_directory: {os.listdir(current_directory)}")
        logging.info(f" - listdir db: {os.listdir(node_db_dir)}")
        return secs_to_start
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )


def create_pgpass_file(env):
    current_directory = os.getcwd()
    os.chdir(ROOT_TEST_PATH)
    db_sync_config_dir = Path.cwd() / 'cardano-db-sync' / 'config'
    os.chdir(db_sync_config_dir)

    pgpass_file = f"pgpass-{env}"
    POSTGRES_PORT = os.getenv('PGPORT')
    pgpass_content = f"{POSTGRES_DIR}:{POSTGRES_PORT}:{env}:{POSTGRES_USER}:*"
    export_env_var("PGPASSFILE", f"config/pgpass-{env}")

    with open(pgpass_file, "w") as pgpass_text_file:
        print(pgpass_content, file=pgpass_text_file)
    os.chmod(pgpass_file, 0o600)
    os.chdir(current_directory)


def create_database(): 
    os.chdir(ROOT_TEST_PATH)
    db_sync_dir = Path.cwd() / 'cardano-db-sync'
    os.chdir(db_sync_dir)

    try:
        cmd = ["scripts/postgresql-setup.sh", "--createdb"]
        output = (
            subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            .decode("utf-8")
            .strip()
        )
        logging.info(f"Create database script output: {output}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )
    if "All good!" not in output:
        raise RuntimeError("Create database has not ended successfully")


def copy_db_sync_executables(build_method="nix"):
    current_directory = os.getcwd()
    os.chdir(ROOT_TEST_PATH)
    db_sync_dir = Path.cwd() / 'cardano-db-sync'
    os.chdir(db_sync_dir)
    
    if build_method == "nix":
        db_sync_binary_location = "db-sync-node/bin/cardano-db-sync"
        db_tool_binary_location = "db-sync-tool/bin/cardano-db-tool"
        shutil.copy2(db_sync_binary_location, "_cardano-db-sync")
        shutil.copy2(db_tool_binary_location, "_cardano-db-tool")
        os.chdir(current_directory)
        return

    try:
        find_db_cmd = [ "find", ".", "-name", "cardano-db-sync", "-executable", "-type", "f" ]
        output_find_db_cmd = (
            subprocess.check_output(find_db_cmd, stderr=subprocess.STDOUT, timeout=15)
            .decode("utf-8")
            .strip()
        )
        os.chdir(current_directory)
        logging.info(f"Find cardano-db-sync output: {output_find_db_cmd}")
        shutil.copy2(output_find_db_cmd, "_cardano-db-sync")

        find_db_tool_cmd = [ "find", ".", "-name", "cardano-db-tool", "-executable", "-type", "f" ]
        output_find_db_tool_cmd = (
            subprocess.check_output(find_db_tool_cmd, stderr=subprocess.STDOUT, timeout=15)
            .decode("utf-8")
            .strip()
        )

        logging.info(f"Find cardano-db-tool output: {output_find_db_tool_cmd}")
        shutil.copy2(output_find_db_tool_cmd, "_cardano-db-tool")

    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )


def get_db_sync_version():
    current_directory = os.getcwd()
    os.chdir(ROOT_TEST_PATH / "cardano-db-sync")
    try:
        cmd = "./_cardano-db-sync --version"
        output = (
            subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
                .decode("utf-8")
                .strip()
        )
        cardano_db_sync_version = output.split("git revision ")[0].strip()
        cardano_db_sync_git_revision = output.split("git revision ")[1].strip()
        os.chdir(current_directory)
        return str(cardano_db_sync_version), str(cardano_db_sync_git_revision)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )


def get_latest_snapshot_url(env, args):
    github_snapshot_url = utils.get_arg_value(args=args, key="snapshot_url")
    if github_snapshot_url != "latest":
        return github_snapshot_url

    if env == "mainnet":
        general_snapshot_url = "https://update-cardano-mainnet.iohk.io/?list-type=2&delimiter=/&prefix=cardano-db-sync/&max-keys=50&cachestamp=459588"
    else:
        raise ValueError('Snapshot are currently available only for mainnet environment')

    headers = {'Content-type': 'application/json'}
    res_with_latest_db_sync_version = requests.get(general_snapshot_url, headers=headers)
    dict_with_latest_db_sync_version = xmltodict.parse(res_with_latest_db_sync_version.content)
    db_sync_latest_version_prefix = dict_with_latest_db_sync_version["ListBucketResult"]["CommonPrefixes"]["Prefix"]

    if env == "mainnet":
        latest_snapshots_list_url = f"https://update-cardano-mainnet.iohk.io/?list-type=2&delimiter=/&prefix={db_sync_latest_version_prefix}&max-keys=50&cachestamp=462903"
    else:
        raise ValueError('Snapshot are currently available only for mainnet environment')

    res_snapshots_list = requests.get(latest_snapshots_list_url, headers=headers)
    dict_snapshots_list = xmltodict.parse(res_snapshots_list.content)
    latest_snapshot = dict_snapshots_list["ListBucketResult"]["Contents"][-2]["Key"]
   
    if env == "mainnet":
        latest_snapshot_url = f"https://update-cardano-mainnet.iohk.io/{latest_snapshot}"
    else:
        raise ValueError('Snapshot are currently available only for mainnet environment')

    return latest_snapshot_url


def download_db_sync_snapshot(snapshot_url):
    current_directory = os.getcwd()
    headers = {'User-Agent': 'Mozilla/5.0'}
    archive_name = snapshot_url.split("/")[-1].strip()

    logging.info("Download db-sync snapshot file:")
    logging.info(f" - current_directory: {current_directory}")
    logging.info(f" - download_url: {snapshot_url}")
    logging.info(f" - archive name: {archive_name}")

    with requests.get(snapshot_url, headers = headers, stream = True, timeout = 60 * 60) as r:
        r.raise_for_status()
        with open(archive_name, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return archive_name


def get_snapshot_sha_256_sum(snapshot_url):
    snapshot_sha_256_sum_url = snapshot_url + ".sha256sum"
    for line in requests.get(snapshot_sha_256_sum_url):
        return line.decode('utf-8').split(" ")[0]


def restore_db_sync_from_snapshot(env, snapshot_file, remove_ledger_dir="yes"):
    os.chdir(ROOT_TEST_PATH)
    if remove_ledger_dir == "yes":
        ledger_state_dir = Path.cwd() / 'cardano-db-sync' / 'ledger-state' / f"{env}"
        remove_dir(ledger_state_dir)
    os.chdir(Path.cwd() / 'cardano-db-sync')
    
    ledger_dir = create_dir(f"ledger-state/{env}")
    logging.info(f"ledger_dir: {ledger_dir}")
    
    # set tmp to local dir in current partition due to buildkite agent space 
    # limitation on /tmp which is not big enough for snapshot restoration
    TMP_DIR=create_dir('tmp')
    export_env_var("TMPDIR", TMP_DIR)

    export_env_var("PGPASSFILE", f"config/pgpass-{env}")
    export_env_var("ENVIRONMENT", f"{env}")
    export_env_var("RESTORE_RECREATE_DB", "N")
    start_restoration = time.perf_counter()

    p = subprocess.Popen(["scripts/postgresql-setup.sh", "--restore-snapshot", f"{snapshot_file}", f"{ledger_dir}"], stdout=subprocess.PIPE)
    try:     
        outs, errs = p.communicate(timeout=36000)
        logging.info = outs.decode("utf-8")
        print(f"Restore database: {output}")
        if errs:
            errors = errs.decode("utf-8")
            logging.error(f"Error during restoration: {errors}")

    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )
    except subprocess.TimeoutExpired as e:
        p.kill()
        logging.error(e)

    finally:
        export_env_var("TMPDIR", "/tmp")

    if "All good!" not in outs.decode("utf-8"):
        raise RuntimeError("Restoration has not ended successfully")

    end_restoration = time.perf_counter()
    return int(end_restoration - start_restoration)


def create_db_sync_snapshot_stage_1(env):
    os.chdir(ROOT_TEST_PATH)
    os.chdir(Path.cwd() / 'cardano-db-sync')
    export_env_var("PGPASSFILE", f"config/pgpass-{env}")

    cmd = f"./_cardano-db-tool prepare-snapshot --state-dir ledger-state/{env}"
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')

    try:
        outs, errs = p.communicate(timeout=300)
        if errs:
            logging.error(f"Warnings or Errors: {errs}")
        final_line_with_script_cmd = outs.split("\n")[2].lstrip()
        logging.info(f"Snapshot Creation - Stage 1 result: {final_line_with_script_cmd}")
        return final_line_with_script_cmd

    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )


def create_db_sync_snapshot_stage_2(stage_2_cmd, env):
    os.chdir(ROOT_TEST_PATH / 'cardano-db-sync')
    export_env_var("PGPASSFILE", f"config/pgpass-{env}")

    try:
        # Running the command and capturing output and error
        result = subprocess.run(
            stage_2_cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=43200  # 12 hours
        )

        logging.info(f"Snapshot Creation - Stage 2 Output:\n{result.stdout}")
        if result.stderr:
            logging.error(f"Warnings or Errors:\n{result.stderr}")
        # Extracting the snapshot path from the last line mentioning 'Created'
        snapshot_line = next(
            (line for line in result.stdout.splitlines() if line.startswith("Created")),
            "Snapshot creation output not found."
        )
        snapshot_path = snapshot_line.split()[
            1] if "Created" in snapshot_line else "Snapshot path unknown"

        return snapshot_path
    except subprocess.TimeoutExpired:
        raise RuntimeError("Snapshot creation timed out.")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Command '{e.cmd}' failed with error: {e.stderr}")

        
def get_db_sync_tip(env):
    p = subprocess.Popen(["psql", "-P", "pager=off", "-qt", "-U", f"{POSTGRES_USER}", "-d", f"{env}",  "-c", "select epoch_no, block_no, slot_no from block order by id desc limit 1;" ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    should_try = True
    counter = 0

    while should_try:
        try:
            outs, errs = p.communicate(timeout=180)
            output_string = outs.decode("utf-8")
            epoch_no, block_no, slot_no = [e.strip() for e in outs.decode("utf-8").split("|")]
            return epoch_no, block_no, slot_no
        except Exception as e:
            if counter > 5:
                should_try = False
                emergency_upload_artifacts(env)
                logging.exception(e)
                p.kill()
                raise
            logging.exception(f"db-sync tip data unavailable, possible postgress failure. Output from psql: {output_string}")
            counter += 1
            logging.exception(e)
            logging.exception(errs)
            time.sleep(ONE_MINUTE)


def get_db_sync_progress(env):
    p = subprocess.Popen(["psql", "-P", "pager=off", "-qt", "-U", f"{POSTGRES_USER}", "-d", f"{env}",  "-c", "select 100 * (extract (epoch from (max (time) at time zone 'UTC')) - extract (epoch from (min (time) at time zone 'UTC'))) / (extract (epoch from (now () at time zone 'UTC')) - extract (epoch from (min (time) at time zone 'UTC'))) as sync_percent from block ;" ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    should_try = True
    counter = 0

    while should_try:
        try:
            outs, errs = p.communicate(timeout=300)
            progress_string = outs.decode("utf-8")
            db_sync_progress = round(float(progress_string), 2)
            return db_sync_progress
        except Exception as e:
            if counter > 5:
                should_try = False
                emergency_upload_artifacts(env)
                p.kill()
                raise
            logging.exception(f"db-sync progress unavailable, possible postgress failure. Output from psql: {progress_string}")
            counter += 1
            time.sleep(ONE_MINUTE)


def wait_for_db_to_sync(env, sync_percentage = 99.9):
    db_sync_perf_stats.clear()
    start_sync = time.perf_counter()
    last_rollback_time = time.perf_counter()
    db_sync_progress = get_db_sync_progress(env)
    buildkite_timeout_in_sec = 1828000
    counter = 0
    rollback_counter = 0
    db_sync_process = get_process_info('cardano-db-sync')
    log_frequency = get_log_output_frequency(env)

    logging.info("--- Db sync monitoring")
    while db_sync_progress < sync_percentage:
        sync_time_in_sec = time.perf_counter() - start_sync
        if sync_time_in_sec + 5 * ONE_MINUTE > buildkite_timeout_in_sec:
            emergency_upload_artifacts(env)
            raise Exception('Emergency uploading artifacts before buid timeout exception...')
        if counter % 5 == 0:
            current_progress = get_db_sync_progress(env)
            if current_progress < db_sync_progress and db_sync_progress > 3:
                logging.info(f"Progress decreasing - current progress: {current_progress} VS previous: {db_sync_progress}.")
                logging.info("Possible rollback... Printing last 10 lines of log")
                print_n_last_lines_from_file(10, DB_SYNC_LOG_FILE)
                if time.perf_counter() - last_rollback_time > 10 * ONE_MINUTE:
                    logging.info("Resetting previous rollback counter as there was no progress decrease for more than 10 minutes")
                    rollback_counter = 0
                last_rollback_time = time.perf_counter()
                rollback_counter += 1
                logging.info(f"Rollback counter: {rollback_counter} out of 15")
            if rollback_counter > 15:
                logging.info(f"Progress decreasing for {rollback_counter * counter} minutes.")
                logging.exception(f"Shutting down all services and emergency uploading artifacts")
                emergency_upload_artifacts(env)
                raise Exception('Rollback taking too long. Shutting down...')
        if counter % log_frequency == 0:
            node_epoch_no, node_block_no, node_hash, node_slot, node_era, node_sync_progress = get_node_tip(env)
            logging.info(f"node progress [%]: {node_sync_progress}, epoch: {node_epoch_no}, block: {node_block_no}, slot: {node_slot}, era: {node_era}")
            epoch_no, block_no, slot_no = get_db_sync_tip(env)
            db_sync_progress = get_db_sync_progress(env)
            sync_time_h_m_s = utils.seconds_to_time(time.perf_counter() - start_sync)
            logging.info(f"db sync progress [%]: {db_sync_progress}, sync time [h:m:s]: {sync_time_h_m_s}, epoch: {epoch_no}, block: {block_no}, slot: {slot_no}")
            print_n_last_lines_from_file(5, DB_SYNC_LOG_FILE)

        try:
            time_point = int(time.perf_counter() - start_sync)
            _, _, slot_no = get_db_sync_tip(env)
            cpu_usage = db_sync_process.cpu_percent(interval=None)
            rss_mem_usage = db_sync_process.memory_info()[0]
        except Exception as e:
            end_sync = time.perf_counter()
            db_full_sync_time_in_secs = int(end_sync - start_sync)
            logging.exception("Unexpected error during sync process")
            logging.exception(e)
            emergency_upload_artifacts(env)
            return db_full_sync_time_in_secs

        stats_data_point = {"time": time_point, "slot_no": slot_no, "cpu_percent_usage": cpu_usage, "rss_mem_usage": rss_mem_usage}
        db_sync_perf_stats.append(stats_data_point)
        write_data_as_json_to_file(DB_SYNC_PERF_STATS_FILE, db_sync_perf_stats)
        time.sleep(ONE_MINUTE)
        counter += 1

    end_sync = time.perf_counter()
    sync_time_seconds = int(end_sync - start_sync)
    logging.info(f"db sync progress [%] before finalizing process: {db_sync_progress}")
    return sync_time_seconds


def get_total_db_size(env):
    os.chdir(ROOT_TEST_PATH / "cardano-db-sync")
    cmd = ["psql", "-P", "pager=off", "-qt", "-U", f"{POSTGRES_USER}", "-d", f"{env}", "-c", f"SELECT pg_size_pretty( pg_database_size('{env}') );" ]
    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        outs, errs = p.communicate(timeout=60)
        if errs:
            logging.error(f"Error in get database size: {errs}")
        return outs.rstrip().strip()
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError) as e:
        p.kill()
        raise
    except Exception as e:
        p.kill()
        raise


def start_db_sync(env, start_args="", first_start="True"):
    current_directory = os.getcwd()
    os.chdir(ROOT_TEST_PATH)
    export_env_var("DB_SYNC_START_ARGS", start_args)
    export_env_var("FIRST_START", f"{first_start}")
    export_env_var("ENVIRONMENT", env)
    export_env_var("LOG_FILEPATH", DB_SYNC_LOG_FILE)

    try:
        cmd = "./sync_tests/scripts/db-sync-start.sh"
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        os.chdir(current_directory)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )

    not_found = True
    counter = 0

    while not_found:
        if counter > 10 * ONE_MINUTE:
            logging.error(f"ERROR: waited {counter} seconds and the db-sync was not started")
            exit(1)

        for proc in process_iter():
            if "cardano-db-sync" in proc.name():
                logging.info(f"db-sync process present: {proc}")
                not_found = False
                return
        logging.info("Waiting for db-sync to start")
        counter += ONE_MINUTE
        time.sleep(ONE_MINUTE)


def get_file_size(file):
    file_stats = os.stat(file)
    file_size_in_mb = int(file_stats.st_size / (1000 * 1000))
    return file_size_in_mb


def is_string_present_in_file(file_to_check, search_string):
    encoded_search_string = str.encode(search_string)
    with open(file_to_check, 'rb', 0) as file, \
        mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as s:
        if s.find(encoded_search_string) != -1:
            s.seek(s.find(encoded_search_string))
            logging.info(s.readline().decode("utf-8"))
            return "Yes"
        return "No"


def are_errors_present_in_db_sync_logs(log_file):
    return is_string_present_in_file(log_file, "db-sync-node:Error")


def are_rollbacks_present_in_db_sync_logs(log_file):
    with open(log_file, 'rb', 0) as file, \
        mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as s:
        initial_rollback_position = s.find(b'rolling')
        offset = s.find(b'rolling', initial_rollback_position + len('rolling'))
        if offset != -1:
            s.seek(offset)
            if s.find(b'rolling'):
                return "Yes"
        return "No"


def setup_postgres(pg_dir=POSTGRES_DIR, pg_user=POSTGRES_USER, pg_port='5432'):
    current_directory = os.getcwd()
    os.chdir(ROOT_TEST_PATH)
    
    export_env_var("POSTGRES_DIR", pg_dir)
    export_env_var("PGHOST", 'localhost')
    export_env_var("PGUSER", pg_user)
    export_env_var("PGPORT", pg_port)

    try:
        cmd = ["./sync_tests/scripts/postgres-start.sh", f"{pg_dir}", "-k"]
        output = (
            subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            .decode("utf-8")
            .strip()
        )
        logging.info(f"Setup postgres script output: {output}")
        os.chdir(current_directory)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' return with error (code {}): {}".format(
                e.cmd, e.returncode, " ".join(str(e.output).split())
            )
        )


def list_databases():
    cmd = ["psql", "-U", f"{POSTGRES_USER}", "-l" ]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')

    try:
        outs, errs = p.communicate(timeout=60)
        logging.info(f"List databases: {outs}")
        if errs:
            logging.error(f"Error in list databases: {errs}")
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError) as e:
        p.kill()
        raise


def get_db_schema():
    try:
        conn = psycopg2.connect(
            database=f'{ENVIRONMENT}', user=f'{POSTGRES_USER}'
        )
        cursor = conn.cursor()
        get_all_tables = 'SELECT table_name FROM information_schema.tables WHERE table_schema=\'public\''
        cursor.execute(get_all_tables)
        tabels = cursor.fetchall();

        db_schema = {}
        for table in tabels:
            table_name = table[0]
            get_table_fields_and_attributes = f'SELECT a.attname as "Column", pg_catalog.format_type(a.atttypid, a.atttypmod) as "Datatype" FROM pg_catalog.pg_attribute a WHERE a.attnum > 0 AND NOT a.attisdropped AND a.attrelid = ( SELECT c.oid FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname ~ \'^{table_name}$\' AND pg_catalog.pg_table_is_visible(c.oid));'
            cursor.execute(get_table_fields_and_attributes)
            table_with_attributes = cursor.fetchall()
            attributes = []
            table_schema = {}
            for row in table_with_attributes:
                attributes.append(row)
                table_schema.update({str(table_name) : attributes }) 
            db_schema.update({str(table_name) : attributes })
        cursor.close()
        conn.commit()
        conn.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        if conn is not None:
            conn.close()

    return db_schema


def get_db_indexes():
    try:
        conn = psycopg2.connect(
            database=f'{ENVIRONMENT}', user=f'{POSTGRES_USER}'
        )
        cursor = conn.cursor()

        get_all_tables = f'select tbl.relname as table_name from pg_index pgi join pg_class idx on idx.oid = pgi.indexrelid join pg_namespace insp on insp.oid = idx.relnamespace join pg_class tbl on tbl.oid = pgi.indrelid join pg_namespace tnsp on tnsp.oid = tbl.relnamespace where pgi.indisunique and tnsp.nspname = \'public\';'
        cursor.execute(get_all_tables)          
        tables = cursor.fetchall()
        all_indexes = {}

        for table in tables:
            table_name =table[0]
            get_table_and_index = f'select tbl.relname as table_name, idx.relname as index_name from pg_index pgi join pg_class idx on idx.oid = pgi.indexrelid join pg_namespace insp on insp.oid = idx.relnamespace join pg_class tbl on tbl.oid = pgi.indrelid join pg_namespace tnsp on tnsp.oid = tbl.relnamespace where pgi.indisunique and tnsp.nspname = \'public\' and tbl.relname = \'{table_name}\';'
            cursor.execute(get_table_and_index)          
            table_and_index = cursor.fetchall()
            indexes = []
            table_indexes = {}
            for table, index in table_and_index:
                indexes.append(index)
                table_indexes.update({str(table_name) : indexes }) 
            all_indexes.update({str(table_name) : indexes })         
        cursor.close()
        conn.commit()
        conn.close()
        return all_indexes
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        if conn is not None:
            conn.close()
            
    return all_indexes


def check_database(fn, err_msg, expected_value):
    try:
        assert_that(fn()).described_as(err_msg).is_equal_to(expected_value)
    except AssertionError as e:
        print_color_log(sh_colors.WARNING, f'Warning - validation errors: {e}\n\n')
        return e