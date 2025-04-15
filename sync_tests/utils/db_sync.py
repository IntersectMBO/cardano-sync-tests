import hashlib
import json
import logging
import os
import pathlib as pl
import platform
import shutil
import subprocess
import sys
import tarfile
import time
import typing as tp
from datetime import timedelta
from os.path import basename
from os.path import normpath
from pathlib import Path

import matplotlib.pyplot as plt
import psutil
import psycopg2
import requests
import xmltodict
from assertpy import assert_that

from sync_tests.utils import helpers
from sync_tests.utils import node

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

ONE_MINUTE = 60
ROOT_TEST_PATH = Path.cwd()

# Environment Variables
ENVIRONMENT = os.getenv("environment")
NODE_PR = os.getenv("node_pr")
NODE_BRANCH = os.getenv("node_branch")
NODE_VERSION = os.getenv("node_version")
DB_SYNC_BRANCH = os.getenv("db_sync_branch")
DB_SYNC_VERSION = os.getenv("db_sync_version")

# System Information
POSTGRES_DIR = ROOT_TEST_PATH.parents[0]
POSTGRES_USER = (
    subprocess.run(["whoami"], stdout=subprocess.PIPE, check=False).stdout.decode("utf-8").strip()
)

# Log and Stats Paths
db_sync_perf_stats: list[dict] = []
DB_SYNC_PERF_STATS_FILE = (
    ROOT_TEST_PATH / f"cardano-db-sync/db_sync_{ENVIRONMENT}_performance_stats.json"
)
NODE_LOG_FILE = ROOT_TEST_PATH / f"cardano-node/node_{ENVIRONMENT}_logfile.log"
DB_SYNC_LOG_FILE = ROOT_TEST_PATH / f"cardano-db-sync/db_sync_{ENVIRONMENT}_logfile.log"
EPOCH_SYNC_TIMES_FILE = ROOT_TEST_PATH / f"cardano-db-sync/epoch_sync_times_{ENVIRONMENT}_dump.json"

# Archive Names
NODE_ARCHIVE_NAME = f"cardano_node_{ENVIRONMENT}_logs.zip"
DB_SYNC_ARCHIVE_NAME = f"cardano_db_sync_{ENVIRONMENT}_logs.zip"
SYNC_DATA_ARCHIVE_NAME = f"epoch_sync_times_{ENVIRONMENT}_dump.zip"
PERF_STATS_ARCHIVE_NAME = f"db_sync_{ENVIRONMENT}_perf_stats.zip"

CHART = f"full_sync_{ENVIRONMENT}_stats_chart.png"


class sh_colors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def print_color_log(log_type: str, message: str) -> None:
    """Log messages with colors for terminal output."""
    print(f"{log_type}{message}{sh_colors.ENDC}")


def get_machine_name() -> str:
    """Retrieve the name of the machine."""
    return platform.node()


def export_env_var(name: str, value: tp.Any) -> None:
    """Export an environment variable with the given name and value."""
    os.environ[name] = str(value)


def wait(seconds: int) -> None:
    """Pause execution for the specified number of seconds."""
    time.sleep(seconds)


def make_tarfile(output_filename: str, source_dir: str) -> None:
    """Create a tar.gz archive of the specified source directory."""
    shutil.make_archive(base_name=output_filename[:-7], format="gztar", root_dir=source_dir)


def upload_artifact(file: str, destination: str = "auto", s3_path: str | None = None) -> None:
    """Upload an artifact to either S3 or Buildkite based on the specified destination."""
    if destination in ("buildkite", "auto"):
        try:
            cmd = ["buildkite-agent", "artifact", "upload", f"{file}"]
            subprocess.run(cmd, check=True)
            logging.info(f"Uploaded {file} to Buildkite.")
        except (subprocess.CalledProcessError, FileNotFoundError):
            logging.warning("Buildkite agent not available. Falling back to S3.")
        else:
            return

    if destination in ("s3", "auto"):
        if not s3_path:
            msg = "S3 path must be specified for S3 uploads."
            raise ValueError(msg)
        try:
            cmd = ["aws", "s3", "cp", f"{file}", f"s3://{s3_path}"]
            subprocess.run(cmd, check=True)
            logging.info(f"Uploaded {file} to S3 at {s3_path}.")
        except subprocess.CalledProcessError:
            logging.exception(f"Error uploading {file} to S3")


def create_node_database_archive(env: str) -> str:
    """Create an archive of the Cardano node database for the specified environment."""
    current_directory = os.getcwd()
    os.chdir(ROOT_TEST_PATH)
    os.chdir(Path.cwd() / "cardano-node")
    node_directory = os.getcwd()
    node_db_archive = f"node-db-{env}.tar.gz"
    make_tarfile(node_db_archive, "db")
    os.chdir(current_directory)
    node_db_archive_path = node_directory + f"/{node_db_archive}"
    return node_db_archive_path


def set_buildkite_meta_data(key: str, value: tp.Any) -> None:
    """Set metadata in Buildkite for the specified key and value."""
    p = subprocess.Popen(["buildkite-agent", "meta-data", "set", f"{key}", f"{value}"])
    p.communicate(timeout=15)


def get_buildkite_meta_data(key: str) -> str:
    """Retrieve metadata from Buildkite for the specified key."""
    p = subprocess.Popen(
        ["buildkite-agent", "meta-data", "get", f"{key}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    outs, errs = p.communicate(timeout=15)
    return outs.decode("utf-8").strip()


def write_data_as_json_to_file(file: str | Path, data: dict | list) -> None:
    """Write data to a file in JSON format."""
    with open(file, "w") as test_results_file:
        json.dump(data, test_results_file, indent=2)


def print_file(file: str | Path, number_of_lines: int = 0) -> None:
    """Print contents of a file to the log, optionally limiting to a specified number of lines."""
    file_path = Path(file) if isinstance(file, str) else file
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(file_path, "a+") as f:
        lines = f.readlines()
    for line in lines[-number_of_lines:] if number_of_lines else lines:
        logging.info(line.strip())


def manage_directory(dir_name: str, action: str, root: str = ".") -> str | None:
    """Manage a directory by creating or removing it based on the action specified."""
    path = Path(f"{root}/{dir_name}")
    if action == "create":
        path.mkdir(parents=True, exist_ok=True)
        return str(path)
    if action == "remove":
        if path.exists():
            shutil.rmtree(path)
        return None
    msg = "Action must be either 'create' or 'remove'."
    raise ValueError(msg)


def get_file_sha_256_sum(filepath: str | Path) -> str:
    """Calculate and returns the SHA-256 checksum of a file."""
    return hashlib.file_digest(Path(filepath).open("rb"), hashlib.sha256).hexdigest()


def print_n_last_lines_from_file(n: int, file_name: str | Path) -> None:
    """Print the last n lines from the specified file."""
    logs = (
        subprocess.run(["tail", "-n", f"{n}", f"{file_name}"], stdout=subprocess.PIPE, check=False)
        .stdout.decode("utf-8")
        .strip()
        .rstrip()
        .splitlines()
    )
    for line in logs:
        logging.info(line)


def get_last_perf_stats_point() -> dict[str, int]:
    """Retrieve the last performance statistics data point, or initializes one if none exists."""
    try:
        last_perf_stats_point = db_sync_perf_stats[-1]
    except Exception:
        logging.exception("Exception in get_last_perf_stats_point")
        stats_data_point = {
            "time": 0,
            "slot_no": 0,
            "cpu_percent_usage": 0,
            "rss_mem_usage": 0,
        }
        db_sync_perf_stats.append(stats_data_point)
        last_perf_stats_point = db_sync_perf_stats[-1]

    return last_perf_stats_point


def get_log_output_frequency(env: str) -> int:
    """Determine the log output frequency based on the environment."""
    if env == "mainnet":
        return 20
    return 3


def export_epoch_sync_times_from_db(
    env: str, file: str | Path, snapshot_epoch_no: int | str = 0
) -> str | None:
    """Export epoch synchronization times from the database to a file."""
    os.chdir(ROOT_TEST_PATH / "cardano-db-sync")
    try:
        p = subprocess.Popen(
            [
                "psql",
                f"{env}",
                "-t",
                "-c",
                rf"\o {file}",
                "-c",
                "SELECT array_to_json(array_agg(epoch_sync_time), FALSE) FROM "
                f"epoch_sync_time where no >= {snapshot_epoch_no};",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        out, err = (p.decode("utf-8").strip() for p in p.communicate(timeout=600))
        if err:
            logging.error(
                f"Error during exporting epoch sync times from db: {err}. "
                "Killing extraction process."
            )
            p.kill()
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        p.kill()
        logging.exception(
            "Error during exporting epoch sync times from db. Killing extraction process."
        )
    except Exception:
        logging.exception(
            "Error during exporting epoch sync times from db. Killing extraction process."
        )
        p.kill()
    else:
        return out

    return None


def emergency_upload_artifacts(env: str) -> None:
    """Upload artifacts for debugging in case of an emergency."""
    write_data_as_json_to_file(DB_SYNC_PERF_STATS_FILE, db_sync_perf_stats)
    export_epoch_sync_times_from_db(env, EPOCH_SYNC_TIMES_FILE)

    helpers.zip_file(PERF_STATS_ARCHIVE_NAME, DB_SYNC_PERF_STATS_FILE)
    helpers.zip_file(SYNC_DATA_ARCHIVE_NAME, EPOCH_SYNC_TIMES_FILE)
    helpers.zip_file(DB_SYNC_ARCHIVE_NAME, DB_SYNC_LOG_FILE)
    helpers.zip_file(NODE_ARCHIVE_NAME, NODE_LOG_FILE)

    upload_artifact(PERF_STATS_ARCHIVE_NAME)
    upload_artifact(SYNC_DATA_ARCHIVE_NAME)
    upload_artifact(DB_SYNC_ARCHIVE_NAME)
    upload_artifact(NODE_ARCHIVE_NAME)

    helpers.manage_process(proc_name="cardano-db-sync", action="terminate")
    helpers.manage_process(proc_name="cardano-node", action="terminate")


def download_and_extract_node_snapshot(env: str) -> None:
    """Download and extracts the Cardano node snapshot for the specified environment."""
    current_directory = os.getcwd()
    headers = {"User-Agent": "Mozilla/5.0"}
    if env == "mainnet":
        snapshot_url = "https://update-cardano-mainnet.iohk.io/cardano-node-state/db-mainnet.tar.gz"
    else:
        snapshot_url = ""  # no other environments are supported for now

    archive_name = f"db-{env}.tar.gz"

    logging.info("Download node snapshot file:")
    logging.info(f" - current_directory: {current_directory}")
    logging.info(f" - download_url: {snapshot_url}")
    logging.info(f" - archive name: {archive_name}")

    with requests.get(snapshot_url, headers=headers, stream=True, timeout=2800) as r:
        r.raise_for_status()
        with open(archive_name, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    logging.info(f" ------ listdir (before archive extraction): {os.listdir(current_directory)}")
    tf = tarfile.open(Path(current_directory) / archive_name)
    tf.extractall(Path(current_directory))
    os.rename(f"db-{env}", "db")
    helpers.delete_file(Path(current_directory) / archive_name)
    logging.info(f" ------ listdir (after archive extraction): {os.listdir(current_directory)}")


def set_node_socket_path_env_var_in_cwd() -> None:
    """Set the node socket path environment variable in the current working directory."""
    os.chdir(ROOT_TEST_PATH / "cardano-node")
    current_directory = os.getcwd()
    if basename(normpath(current_directory)) != "cardano-node":
        msg = f"You're not inside 'cardano-node' directory but in: {current_directory}"
        raise Exception(msg)
    socket_path = "db/node.socket"
    export_env_var("CARDANO_NODE_SOCKET_PATH", socket_path)


def create_pgpass_file(env: str) -> None:
    """Create a PostgreSQL password file for the specified environment."""
    current_directory = os.getcwd()
    os.chdir(ROOT_TEST_PATH)
    db_sync_config_dir = Path.cwd() / "cardano-db-sync" / "config"
    os.chdir(db_sync_config_dir)

    pgpass_file = f"pgpass-{env}"
    postgres_port = os.getenv("PGPORT")
    pgpass_content = f"{POSTGRES_DIR}:{postgres_port}:{env}:{POSTGRES_USER}:*"
    export_env_var("PGPASSFILE", f"config/pgpass-{env}")

    with open(pgpass_file, "w") as pgpass_text_file:
        print(pgpass_content, file=pgpass_text_file)
    os.chmod(pgpass_file, 0o600)
    os.chdir(current_directory)


def create_database() -> None:
    """Set up the PostgreSQL database for use with Cardano DB Sync."""
    os.chdir(ROOT_TEST_PATH)
    db_sync_dir = Path.cwd() / "cardano-db-sync"
    os.chdir(db_sync_dir)

    try:
        cmd = ["scripts/postgresql-setup.sh", "--createdb"]
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode("utf-8").strip()
        logging.info(f"Create database script output: {output}")
    except subprocess.CalledProcessError as e:
        msg = "command '{}' return with error (code {}): {}".format(
            e.cmd, e.returncode, " ".join(str(e.output).split())
        )
        raise RuntimeError(msg) from e
    if "All good!" not in output:
        msg = "Create database has not ended successfully"
        raise RuntimeError(msg)


def copy_db_sync_executables(build_method: str = "nix") -> None:
    """Copy the Cardano DB Sync executables built with the specified build method."""
    current_directory = os.getcwd()
    os.chdir(ROOT_TEST_PATH)
    db_sync_dir = Path.cwd() / "cardano-db-sync"
    os.chdir(db_sync_dir)

    if build_method == "nix":
        db_sync_binary_location = "db-sync-node/bin/cardano-db-sync"
        db_tool_binary_location = "db-sync-tool/bin/cardano-db-tool"
        shutil.copy2(db_sync_binary_location, "_cardano-db-sync")
        shutil.copy2(db_tool_binary_location, "_cardano-db-tool")
        os.chdir(current_directory)
        return

    try:
        find_db_cmd = [
            "find",
            ".",
            "-name",
            "cardano-db-sync",
            "-executable",
            "-type",
            "f",
        ]
        output_find_db_cmd = (
            subprocess.check_output(find_db_cmd, stderr=subprocess.STDOUT, timeout=15)
            .decode("utf-8")
            .strip()
        )
        os.chdir(current_directory)
        logging.info(f"Find cardano-db-sync output: {output_find_db_cmd}")
        shutil.copy2(output_find_db_cmd, "_cardano-db-sync")

        find_db_tool_cmd = [
            "find",
            ".",
            "-name",
            "cardano-db-tool",
            "-executable",
            "-type",
            "f",
        ]
        output_find_db_tool_cmd = (
            subprocess.check_output(find_db_tool_cmd, stderr=subprocess.STDOUT, timeout=15)
            .decode("utf-8")
            .strip()
        )

        logging.info(f"Find cardano-db-tool output: {output_find_db_tool_cmd}")
        shutil.copy2(output_find_db_tool_cmd, "_cardano-db-tool")

    except subprocess.CalledProcessError as e:
        msg = "command '{}' return with error (code {}): {}".format(
            e.cmd, e.returncode, " ".join(str(e.output).split())
        )
        raise RuntimeError(msg) from e


def get_db_sync_version() -> tuple[str, str]:
    """Retrieve the version of the Cardano DB Sync executable."""
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
        msg = "command '{}' return with error (code {}): {}".format(
            e.cmd, e.returncode, " ".join(str(e.output).split())
        )
        raise RuntimeError(msg) from e


def get_latest_snapshot_url(env: str, args: tp.Any) -> str:
    """Fetch the latest snapshot URL for the specified environment."""
    github_snapshot_url: str = helpers.get_arg_value(args=args, key="snapshot_url")
    if github_snapshot_url != "latest":
        return github_snapshot_url

    if env == "mainnet":
        general_snapshot_url = "https://update-cardano-mainnet.iohk.io/?list-type=2&delimiter=/&prefix=cardano-db-sync/&max-keys=50&cachestamp=459588"
    else:
        msg = "Snapshot are currently available only for mainnet environment"
        raise ValueError(msg)

    headers = {"Content-type": "application/json"}
    res_with_latest_db_sync_version = requests.get(general_snapshot_url, headers=headers)
    dict_with_latest_db_sync_version = xmltodict.parse(res_with_latest_db_sync_version.content)
    db_sync_latest_version_prefix = dict_with_latest_db_sync_version["ListBucketResult"][
        "CommonPrefixes"
    ]["Prefix"]

    if env == "mainnet":
        latest_snapshots_list_url = f"https://update-cardano-mainnet.iohk.io/?list-type=2&delimiter=/&prefix={db_sync_latest_version_prefix}&max-keys=50&cachestamp=462903"
    else:
        msg = "Snapshot are currently available only for mainnet environment"
        raise ValueError(msg)

    res_snapshots_list = requests.get(latest_snapshots_list_url, headers=headers)
    dict_snapshots_list = xmltodict.parse(res_snapshots_list.content)
    latest_snapshot = dict_snapshots_list["ListBucketResult"]["Contents"][-2]["Key"]

    if env == "mainnet":
        latest_snapshot_url = f"https://update-cardano-mainnet.iohk.io/{latest_snapshot}"
    else:
        msg = "Snapshot are currently available only for mainnet environment"
        raise ValueError(msg)

    return latest_snapshot_url


def download_db_sync_snapshot(snapshot_url: str) -> str:
    """Download the database synchronization snapshot from a given URL."""
    current_directory = os.getcwd()
    headers = {"User-Agent": "Mozilla/5.0"}
    archive_name = snapshot_url.split("/")[-1].strip()

    logging.info("Download db-sync snapshot file:")
    logging.info(f" - current_directory: {current_directory}")
    logging.info(f" - download_url: {snapshot_url}")
    logging.info(f" - archive name: {archive_name}")

    with requests.get(snapshot_url, headers=headers, stream=True, timeout=60 * 60) as r:
        r.raise_for_status()
        with open(archive_name, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return archive_name


def get_snapshot_sha_256_sum(snapshot_url: str) -> str | None:
    """Calculate the SHA-256 checksum of the downloaded snapshot."""
    snapshot_sha_256_sum_url = snapshot_url + ".sha256sum"
    for line in requests.get(snapshot_sha_256_sum_url):
        return line.decode("utf-8").split(" ")[0]
    return None


def restore_db_sync_from_snapshot(
    env: str, snapshot_file: str | Path, remove_ledger_dir: str = "yes"
) -> int:
    """Restore the Cardano DB Sync database from a snapshot."""
    os.chdir(ROOT_TEST_PATH)
    if remove_ledger_dir == "yes":
        ledger_state_dir = Path.cwd() / "cardano-db-sync" / "ledger-state" / f"{env}"
        # TODO: Fix this, as it will not remove the directory. It is passing absolute path, so
        # it cannot work with the default `root` set to `.`.
        manage_directory(dir_name=str(ledger_state_dir), action="remove")
    os.chdir(Path.cwd() / "cardano-db-sync")

    ledger_dir = manage_directory(dir_name=f"ledger-state/{env}", action="create")
    logging.info(f"ledger_dir: {ledger_dir}")

    # set tmp to local dir in current partition due to buildkite agent space
    # limitation on /tmp which is not big enough for snapshot restoration
    tmp_dir = manage_directory(dir_name="tmp", action="create")
    export_env_var("TMPDIR", tmp_dir)

    export_env_var("PGPASSFILE", f"config/pgpass-{env}")
    export_env_var("ENVIRONMENT", f"{env}")
    export_env_var("RESTORE_RECREATE_DB", "N")
    start_restoration = time.perf_counter()

    p = subprocess.Popen(
        [
            "scripts/postgresql-setup.sh",
            "--restore-snapshot",
            f"{snapshot_file}",
            f"{ledger_dir}",
        ],
        stdout=subprocess.PIPE,
    )
    try:
        outs, errs = p.communicate(timeout=36000)
        output = outs.decode("utf-8")
        print(f"Restore database: {output}")
        if errs:
            errors = errs.decode("utf-8")
            logging.error(f"Error during restoration: {errors}")

    except subprocess.CalledProcessError as e:
        msg = "command '{}' return with error (code {}): {}".format(
            e.cmd, e.returncode, " ".join(str(e.output).split())
        )
        raise RuntimeError(msg) from e
    except subprocess.TimeoutExpired:
        p.kill()
        logging.exception("Process timeout expired")

    finally:
        export_env_var("TMPDIR", "/tmp")

    if "All good!" not in outs.decode("utf-8"):
        msg = "Restoration has not ended successfully"
        raise RuntimeError(msg)

    end_restoration = time.perf_counter()
    return int(end_restoration - start_restoration)


def create_db_sync_snapshot_stage_1(env: str) -> str:
    """Perform the first stage of creating a DB Sync snapshot."""
    os.chdir(ROOT_TEST_PATH)
    os.chdir(Path.cwd() / "cardano-db-sync")
    export_env_var("PGPASSFILE", f"config/pgpass-{env}")

    cmd = f"./_cardano-db-tool prepare-snapshot --state-dir ledger-state/{env}"
    p = subprocess.Popen(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    )

    try:
        outs, errs = p.communicate(timeout=300)
        if errs:
            logging.error(f"Warnings or Errors: {errs}")
        final_line_with_script_cmd = outs.split("\n")[2].lstrip()
        logging.info(f"Snapshot Creation - Stage 1 result: {final_line_with_script_cmd}")
    except subprocess.CalledProcessError as e:
        msg = "command '{}' return with error (code {}): {}".format(
            e.cmd, e.returncode, " ".join(str(e.output).split())
        )
        raise RuntimeError(msg) from e
    else:
        return final_line_with_script_cmd


def create_db_sync_snapshot_stage_2(stage_2_cmd: str, env: str) -> str:
    """Perform the second stage of creating a DB Sync snapshot."""
    os.chdir(ROOT_TEST_PATH / "cardano-db-sync")
    export_env_var("PGPASSFILE", f"config/pgpass-{env}")

    try:
        # Running the command and capturing output and error
        result = subprocess.run(
            stage_2_cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=43200,
            check=False,  # 12 hours
        )

        logging.info(f"Snapshot Creation - Stage 2 Output:\n{result.stdout}")
        if result.stderr:
            logging.error(f"Warnings or Errors:\n{result.stderr}")
        # Extracting the snapshot path from the last line mentioning 'Created'
        snapshot_line = next(
            (line for line in result.stdout.splitlines() if line.startswith("Created")),
            "Snapshot creation output not found.",
        )
        snapshot_path = (
            snapshot_line.split()[1] if "Created" in snapshot_line else "Snapshot path unknown"
        )
    except subprocess.TimeoutExpired as e:
        msg = "Snapshot creation timed out."
        raise RuntimeError(msg) from e
    except subprocess.CalledProcessError as e:
        msg = f"Command '{e.cmd}' failed with error: {e.stderr}"
        raise RuntimeError(msg) from e
    else:
        return snapshot_path


def get_db_sync_tip(env: str) -> tuple[str, str, str] | None:
    """Retrieve the tip information from the Cardano DB Sync database."""
    p = subprocess.Popen(
        [
            "psql",
            "-P",
            "pager=off",
            "-qt",
            "-U",
            f"{POSTGRES_USER}",
            "-d",
            f"{env}",
            "-c",
            "select epoch_no, block_no, slot_no from block order by id desc limit 1;",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    should_try = True
    counter = 0

    while should_try:
        output_string = ""
        try:
            outs, errs = p.communicate(timeout=180)
            output_string = outs.decode("utf-8")
            epoch_no, block_no, slot_no = [e.strip() for e in outs.decode("utf-8").split("|")]
        except Exception:
            if counter > 5:
                should_try = False
                emergency_upload_artifacts(env)
                logging.exception("Failed to get the tip")
                p.kill()
                raise
            logging.exception(
                f"db-sync tip data unavailable, possible postgress failure. "
                f"Output from psql: {output_string}, errs: {errs.decode('utf-8')}"
            )
            counter += 1
            time.sleep(ONE_MINUTE)
        else:
            return epoch_no, block_no, slot_no

    return None


def get_db_sync_progress(env: str) -> float | None:
    """Calculate the synchronization progress of the Cardano DB Sync database."""
    p = subprocess.Popen(
        [
            "psql",
            "-P",
            "pager=off",
            "-qt",
            "-U",
            f"{POSTGRES_USER}",
            "-d",
            f"{env}",
            "-c",
            "select 100 * (extract (epoch from (max (time) at time zone 'UTC')) "
            "- extract (epoch from (min (time) at time zone 'UTC'))) "
            "/ (extract (epoch from (now () at time zone 'UTC')) "
            "- extract (epoch from (min (time) at time zone 'UTC'))) as sync_percent from block ;",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    should_try = True
    counter = 0

    while should_try:
        progress_string = ""
        try:
            outs, errs = p.communicate(timeout=300)
            progress_string = outs.decode("utf-8")
            db_sync_progress = round(float(progress_string), 2)
        except Exception:
            if counter > 5:
                should_try = False
                emergency_upload_artifacts(env)
                p.kill()
                raise
            logging.exception(
                "db-sync progress unavailable, possible postgress failure. "
                f"Output from psql: {progress_string}"
            )
            counter += 1
            time.sleep(ONE_MINUTE)
        else:
            return db_sync_progress
    return None


def wait_for_db_to_sync(env: str, sync_percentage: float = 99.9) -> int:
    """Wait for the Cardano DB Sync database to fully synchronize."""
    db_sync_perf_stats.clear()
    start_sync = time.perf_counter()
    last_rollback_time = time.perf_counter()
    db_sync_progress = get_db_sync_progress(env)
    assert db_sync_progress is not None  # TODO: refactor
    buildkite_timeout_in_sec = 1828000
    counter = 0
    rollback_counter = 0

    db_sync_process = helpers.manage_process(proc_name="cardano-db-sync", action="get")
    log_frequency = get_log_output_frequency(env)

    logging.info("--- Db sync monitoring")
    while db_sync_progress < sync_percentage:
        sync_time_in_sec = time.perf_counter() - start_sync
        if sync_time_in_sec + 5 * ONE_MINUTE > buildkite_timeout_in_sec:
            emergency_upload_artifacts(env)
            msg = "Emergency uploading artifacts before buid timeout exception..."
            raise Exception(msg)
        if counter % 5 == 0:
            current_progress = get_db_sync_progress(env)
            assert current_progress is not None  # TODO: refactor
            if current_progress < db_sync_progress and db_sync_progress > 3:
                logging.info(
                    "Progress decreasing - current progress: "
                    f"{current_progress} VS previous: {db_sync_progress}."
                )
                logging.info("Possible rollback... Printing last 10 lines of log")
                print_n_last_lines_from_file(10, DB_SYNC_LOG_FILE)
                if time.perf_counter() - last_rollback_time > 10 * ONE_MINUTE:
                    logging.info(
                        "Resetting previous rollback counter as there was no progress decrease "
                        "for more than 10 minutes"
                    )
                    rollback_counter = 0
                last_rollback_time = time.perf_counter()
                rollback_counter += 1
                logging.info(f"Rollback counter: {rollback_counter} out of 15")
            if rollback_counter > 15:
                logging.info(f"Progress decreasing for {rollback_counter * counter} minutes.")
                logging.exception("Shutting down all services and emergency uploading artifacts")
                emergency_upload_artifacts(env)
                msg = "Rollback taking too long. Shutting down..."
                raise Exception(msg)
        if counter % log_frequency == 0:
            tip = node.get_current_tip(env)
            logging.info(
                f"node progress [%]: {tip.sync_progress}, epoch: {tip.epoch}, "
                f"block: {tip.block}, slot: {tip.slot}, era: {tip.era}"
            )
            db_sync_tip = get_db_sync_tip(env)
            assert db_sync_tip is not None  # TODO: refactor
            epoch_no, block_no, slot_no = db_sync_tip
            db_sync_progress = get_db_sync_progress(env)
            assert db_sync_progress is not None  # TODO: refactor
            sync_time_h_m_s = str(timedelta(seconds=(time.perf_counter() - start_sync)))
            logging.info(
                f"db sync progress [%]: {db_sync_progress}, sync time [h:m:s]: {sync_time_h_m_s}, "
                f"epoch: {epoch_no}, block: {block_no}, slot: {slot_no}"
            )
            print_n_last_lines_from_file(5, DB_SYNC_LOG_FILE)

        try:
            time_point = int(time.perf_counter() - start_sync)
            db_sync_tip = get_db_sync_tip(env)
            assert db_sync_tip is not None  # TODO: refactor
            _, _, slot_no = db_sync_tip
            cpu_usage = db_sync_process.cpu_percent(interval=None)
            rss_mem_usage = db_sync_process.memory_info()[0]
        except Exception:
            end_sync = time.perf_counter()
            db_full_sync_time_in_secs = int(end_sync - start_sync)
            logging.exception("Unexpected error during sync process")
            emergency_upload_artifacts(env)
            return db_full_sync_time_in_secs

        stats_data_point = {
            "time": time_point,
            "slot_no": slot_no,
            "cpu_percent_usage": cpu_usage,
            "rss_mem_usage": rss_mem_usage,
        }
        db_sync_perf_stats.append(stats_data_point)
        write_data_as_json_to_file(DB_SYNC_PERF_STATS_FILE, db_sync_perf_stats)
        time.sleep(ONE_MINUTE)
        counter += 1

    end_sync = time.perf_counter()
    sync_time_seconds = int(end_sync - start_sync)
    logging.info(f"db sync progress [%] before finalizing process: {db_sync_progress}")
    return sync_time_seconds


def get_total_db_size(env: str) -> str:
    """Fetch the total size of the Cardano DB Sync database."""
    os.chdir(ROOT_TEST_PATH / "cardano-db-sync")
    cmd = [
        "psql",
        "-P",
        "pager=off",
        "-qt",
        "-U",
        f"{POSTGRES_USER}",
        "-d",
        f"{env}",
        "-c",
        f"SELECT pg_size_pretty( pg_database_size('{env}') );",
    ]
    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
        outs, errs = p.communicate(timeout=60)
        if errs:
            logging.error(f"Error in get database size: {errs}")
        return outs.rstrip().strip()
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        p.kill()
        raise
    except Exception:
        p.kill()
        raise


def start_db_sync(env: str, start_args: str = "", first_start: str = "True") -> None:
    """Start the Cardano DB Sync process."""
    current_directory = os.getcwd()
    os.chdir(ROOT_TEST_PATH)
    export_env_var("DB_SYNC_START_ARGS", start_args)
    export_env_var("FIRST_START", f"{first_start}")
    export_env_var("ENVIRONMENT", env)
    export_env_var("LOG_FILEPATH", DB_SYNC_LOG_FILE)

    try:
        cmd = "./sync_tests/scripts/db-sync-start.sh"
        subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        os.chdir(current_directory)
    except subprocess.CalledProcessError as e:
        msg = "command '{}' return with error (code {}): {}".format(
            e.cmd, e.returncode, " ".join(str(e.output).split())
        )
        raise RuntimeError(msg) from e

    not_found = True
    counter = 0

    while not_found:
        if counter > 10 * ONE_MINUTE:
            logging.error(f"ERROR: waited {counter} seconds and the db-sync was not started")
            sys.exit(1)

        for proc in psutil.process_iter():
            if "cardano-db-sync" in proc.name():
                logging.info(f"db-sync process present: {proc}")
                not_found = False
                return
        logging.info("Waiting for db-sync to start")
        counter += ONE_MINUTE
        time.sleep(ONE_MINUTE)


def get_file_size(file: str) -> int:
    """Return the size of a specified file in megabytes."""
    file_stats = os.stat(file)
    file_size_in_mb = int(file_stats.st_size / (1000 * 1000))
    return file_size_in_mb


def setup_postgres(
    pg_dir: Path = POSTGRES_DIR, pg_user: str = POSTGRES_USER, pg_port: str = "5432"
) -> None:
    """Set up PostgreSQL for use with Cardano DB Sync."""
    current_directory = os.getcwd()
    os.chdir(ROOT_TEST_PATH)

    export_env_var("POSTGRES_DIR", pg_dir)
    export_env_var("PGHOST", "localhost")
    export_env_var("PGUSER", pg_user)
    export_env_var("PGPORT", pg_port)

    try:
        cmd = ["./sync_tests/scripts/postgres-start.sh", f"{pg_dir}", "-k"]
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode("utf-8").strip()
        logging.info(f"Setup postgres script output: {output}")
        os.chdir(current_directory)
    except subprocess.CalledProcessError as e:
        msg = "command '{}' return with error (code {}): {}".format(
            e.cmd, e.returncode, " ".join(str(e.output).split())
        )
        raise RuntimeError(msg) from e


def list_databases() -> None:
    """List all databases available in the PostgreSQL instance."""
    cmd = ["psql", "-U", f"{POSTGRES_USER}", "-l"]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")

    try:
        outs, errs = p.communicate(timeout=60)
        logging.info(f"List databases: {outs}")
        if errs:
            logging.error(f"Error in list databases: {errs}")
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        p.kill()
        raise


def get_db_schema() -> dict:
    """Retrieve the schema of the Cardano DB Sync database."""
    try:
        conn = psycopg2.connect(database=f"{ENVIRONMENT}", user=f"{POSTGRES_USER}")
        cursor = conn.cursor()
        get_all_tables = (
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        )
        cursor.execute(get_all_tables)
        tabels = cursor.fetchall()

        db_schema = {}
        for table in tabels:
            table_name = table[0]
            get_table_fields_and_attributes = (
                'SELECT a.attname as "Column", pg_catalog.format_type(a.atttypid, a.atttypmod)'
                ' as "Datatype" FROM pg_catalog.pg_attribute a WHERE a.attnum > 0'
                " AND NOT a.attisdropped AND a.attrelid = ( SELECT c.oid"
                " FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n"
                f" ON n.oid = c.relnamespace WHERE c.relname ~ '^{table_name}$'"
                " AND pg_catalog.pg_table_is_visible(c.oid));"
            )
            cursor.execute(get_table_fields_and_attributes)
            table_with_attributes = cursor.fetchall()
            attributes = []
            table_schema = {}
            for row in table_with_attributes:
                attributes.append(row)
                table_schema.update({str(table_name): attributes})
            db_schema.update({str(table_name): attributes})
        cursor.close()
        conn.commit()
        conn.close()
    except (Exception, psycopg2.DatabaseError):
        logging.exception("Error")
    finally:
        if conn is not None:
            conn.close()

    return db_schema


def get_db_indexes() -> dict:
    """Fetch the indexes of tables in the Cardano DB Sync database."""
    try:
        conn = psycopg2.connect(database=f"{ENVIRONMENT}", user=f"{POSTGRES_USER}")
        cursor = conn.cursor()

        get_all_tables = (
            "select tbl.relname as table_name from pg_index pgi"
            " join pg_class idx on idx.oid = pgi.indexrelid join pg_namespace insp"
            " on insp.oid = idx.relnamespace join pg_class tbl"
            " on tbl.oid = pgi.indrelid join pg_namespace tnsp"
            " on tnsp.oid = tbl.relnamespace where pgi.indisunique and tnsp.nspname = 'public';"
        )
        cursor.execute(get_all_tables)
        tables = cursor.fetchall()
        all_indexes = {}

        for table in tables:
            table_name = table[0]
            get_table_and_index = (
                "select tbl.relname as table_name, idx.relname as index_name"
                " from pg_index pgi join pg_class idx on idx.oid = pgi.indexrelid"
                " join pg_namespace insp on insp.oid = idx.relnamespace join pg_class tbl"
                " on tbl.oid = pgi.indrelid join pg_namespace tnsp"
                " on tnsp.oid = tbl.relnamespace where pgi.indisunique"
                f" and tnsp.nspname = 'public' and tbl.relname = '{table_name}';"
            )
            cursor.execute(get_table_and_index)
            table_and_index = cursor.fetchall()
            indexes = []
            table_indexes = {}
            for _table, index in table_and_index:
                indexes.append(index)
                table_indexes.update({str(table_name): indexes})
            all_indexes.update({str(table_name): indexes})
        cursor.close()
        conn.commit()
        conn.close()
    except (Exception, psycopg2.DatabaseError):
        logging.exception("Error")
    else:
        return all_indexes
    finally:
        if conn is not None:
            conn.close()

    return all_indexes


def check_database(fn: tp.Callable, err_msg: str, expected_value: tp.Any) -> Exception | None:
    """Validate the database using a specified function and expected value."""
    try:
        assert_that(fn()).described_as(err_msg).is_equal_to(expected_value)
    except AssertionError as e:
        print_color_log(sh_colors.WARNING, f"Warning - validation errors: {e}\n\n")
        return e
    return None


def create_sync_stats_chart() -> None:
    os.chdir(ROOT_TEST_PATH)
    os.chdir(pl.Path.cwd() / "cardano-db-sync")
    fig = plt.figure(figsize=(14, 10))

    # define epochs sync times chart
    ax_epochs = fig.add_axes((0.05, 0.05, 0.9, 0.35))
    ax_epochs.set(xlabel="epochs [number]", ylabel="time [min]")
    ax_epochs.set_title("Epochs Sync Times")

    with open(EPOCH_SYNC_TIMES_FILE) as json_db_dump_file:
        epoch_sync_times = json.load(json_db_dump_file)

    epochs = [e["no"] for e in epoch_sync_times]
    epoch_times = [e["seconds"] / 60 for e in epoch_sync_times]
    ax_epochs.bar(epochs, epoch_times)

    # define performance chart
    ax_perf = fig.add_axes((0.05, 0.5, 0.9, 0.45))
    ax_perf.set(xlabel="time [min]", ylabel="RSS [B]")
    ax_perf.set_title("RSS usage")

    with open(DB_SYNC_PERF_STATS_FILE) as json_db_dump_file:
        perf_stats = json.load(json_db_dump_file)

    times = [e["time"] / 60 for e in perf_stats]
    rss_mem_usage = [e["rss_mem_usage"] for e in perf_stats]

    ax_perf.plot(times, rss_mem_usage)
    fig.savefig(CHART)
