import dataclasses
import datetime
import fileinput
import functools
import json
import logging
import os
import pathlib as pl
import platform
import shutil
import subprocess
import time
import typing as tp
import urllib.request

import git

from sync_tests.utils import blockfrost
from sync_tests.utils import cli
from sync_tests.utils import exceptions
from sync_tests.utils import explorer
from sync_tests.utils import gitpython
from sync_tests.utils import helpers

LOGGER = logging.getLogger(__name__)

CONFIGS_BASE_URL = "https://book.play.dev.cardano.org/environments"
NODE_LOG_FILE_NAME = "logfile.log"


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


def add_to_path(path: pl.Path) -> None:
    """Add a directory to the system PATH environment variable."""
    os.environ["PATH"] = str(path.absolute()) + os.pathsep + os.environ["PATH"]


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


def download_config_file(config_slug: str, save_as: pl.Path) -> None:
    url = f"{CONFIGS_BASE_URL}/{config_slug}"
    LOGGER.info(f"Downloading '{url}' and saving as '{save_as}'")
    urllib.request.urlretrieve(url, save_as)


def get_node_config_files(
    env: str, node_topology_type: str, conf_dir: pl.Path, use_genesis_mode: bool = False
) -> None:
    """Download Cardano node configuration files for the specified environment."""
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


def delete_node_files() -> None:
    for p in pl.Path("..").glob("cardano-*"):
        if p.is_dir():
            LOGGER.info(f"deleting directory: {p}")
            shutil.rmtree(p)  # Use shutil.rmtree to delete directories
        else:
            LOGGER.info(f"deleting file: {p}")
            p.unlink(missing_ok=True)


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
    """Retrieve the current tip of the Cardano node."""
    cmd = ["cardano-cli", "latest", "query", "tip", *get_testnet_args(env=env)]

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
        cmd = "cardano-cli --version"
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
    base_dir: pl.Path, node_start_arguments: tp.Iterable[str]
) -> tuple[subprocess.Popen, tp.IO[str]]:
    start_args = " ".join(node_start_arguments)

    if platform.system().lower() == "windows":
        cmd = (
            "cardano-node run --topology topology.json "
            f"--database-path {base_dir / 'db'} "
            "--host-addr 0.0.0.0 "
            "--port 3000 "
            "--socket-path \\\\.\\pipe\\cardano-node "
            f"--config config.json {start_args}"
        ).strip()
    else:
        socket_path = os.environ.get("CARDANO_NODE_SOCKET_PATH") or ""
        cmd = (
            "cardano-node run --topology topology.json --database-path "
            f"{base_dir / 'db'} "
            "--host-addr 0.0.0.0 --port 3000 --config "
            f"config.json --socket-path {socket_path} {start_args}"
        ).strip()

    LOGGER.info(f"Starting node with cmd: {cmd}")
    logfile = open(base_dir / NODE_LOG_FILE_NAME, "w+")

    proc = subprocess.Popen(cmd.split(" "), stdout=logfile, stderr=logfile)
    return proc, logfile


def wait_node_start(env: str, timeout_minutes: int = 20) -> int:
    """Wait for the Cardano node to start."""
    # when starting from clean state it might take ~30 secs for the cli to work
    # when starting from existing state it might take >10 mins for the cli to work (opening db and
    # replaying the ledger)
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


def rm_node_config_files(conf_dir: pl.Path) -> None:
    LOGGER.info("Removing existing config files")
    for gen in conf_dir.glob("*-genesis.json"):
        pl.Path(gen).unlink(missing_ok=True)
    for f in ("config.json", "topology.json"):
        (conf_dir / f).unlink(missing_ok=True)


def get_epoch_no_d_zero(env: str) -> int:
    if env == "mainnet":
        return 257
    return -1


def get_start_slot_no_d_zero(env: str) -> int:
    if env == "mainnet":
        return 25661009
    return -1


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


def find_genesis_json(era_name: str) -> pl.Path:
    """Find genesis JSON file in state dir."""
    cwd = pl.Path.cwd()
    potential = [
        *cwd.glob(f"*{era_name}*genesis.json"),
        *cwd.glob(f"*genesis*{era_name}.json"),
    ]
    if not potential:
        msg = f"The genesis JSON file not found in `{cwd}`."
        raise exceptions.SyncError(msg)

    genesis_json = potential[0]
    LOGGER.debug(f"Using genesis JSON file `{genesis_json}")
    return genesis_json


def get_genesis(era_name: str) -> dict:
    genesis_file = find_genesis_json(era_name=era_name)
    with open(genesis_file, encoding="utf-8") as in_json:
        genesis_dict: dict = json.load(in_json)
    return genesis_dict


@functools.cache
def get_byron_slot_ln() -> int:
    genesis = get_genesis(era_name="byron")
    return int(genesis["protocolConsts"]["k"]) * 10


@functools.cache
def get_shelley_slot_ln() -> int:
    genesis = get_genesis(era_name="shelley")
    return int(genesis["epochLength"])


def get_no_of_slots_in_era(era_name: str, no_of_epochs_in_era: int) -> int:
    """Get the number of slots in an era."""
    era_name = era_name.lower()
    epoch_length_slots = get_byron_slot_ln() if era_name == "byron" else get_shelley_slot_ln()
    return int(epoch_length_slots * no_of_epochs_in_era)


def wait_for_node_to_sync(env: str, base_dir: pl.Path) -> tuple:
    """Wait for the Cardano node to start."""
    LOGGER.info("Waiting for the node to sync")
    era_details_dict = {}
    epoch_details_dict = {}

    # Get the initial tip data and calculated slot
    actual_epoch, actual_block, actual_hash, actual_slot, actual_era, sync_progress = (
        get_current_tip(env=env)
    )
    last_slot_no = get_calculated_slot_no(env=env) if sync_progress is None else -1
    start_sync = time.perf_counter()
    count = 0

    while True:
        # Log status every 60 iterations.
        if count % 60 == 0:
            now_log = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
            LOGGER.warning(
                f"{now_log} - actual_era  : {actual_era} "
                f" - actual_epoch: {actual_epoch} "
                f" - actual_block: {actual_block} "
                f" - actual_slot : {actual_slot} "
                f" - syncProgress: {sync_progress}",
            )

        # Use the same current time for both era and epoch updates.
        current_time_str = datetime.datetime.now(tz=datetime.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        # If we see a new era, record its starting details.
        if actual_era not in era_details_dict:
            if env == "mainnet":
                actual_era_start_time = blockfrost.get_epoch_start_datetime(epoch_no=actual_epoch)
            else:
                actual_era_start_time = explorer.get_epoch_start_datetime_from_explorer(
                    env=env, epoch_no=actual_epoch
                )
            era_details_dict[actual_era] = {
                "start_epoch": actual_epoch,
                "start_time": actual_era_start_time,
                "start_sync_time": current_time_str,
            }

        # If we see a new epoch, record its starting sync time.
        if actual_epoch not in epoch_details_dict:
            epoch_details_dict[actual_epoch] = {"start_sync_time": current_time_str}

        # Check termination condition:
        # For nodes reporting sync progress, we wait until progress reaches 100.
        if sync_progress is not None and sync_progress >= 100:
            break
        # Otherwise (for nodes without sync progress) wait until the slot number passes
        # the calculated value.
        if sync_progress is None and actual_slot > last_slot_no:
            break

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

    done_time_str = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    end_sync = time.perf_counter()
    sync_time_seconds = int(end_sync - start_sync)
    LOGGER.info(f"sync_time_seconds: {sync_time_seconds}")

    chunk_files = sorted((base_dir / "db" / "immutable").iterdir(), key=lambda f: f.stat().st_mtime)
    latest_chunk_no = chunk_files[-1].stem
    LOGGER.info(f"Sync done!; latest_chunk_no: {latest_chunk_no}")

    # Add "end_sync_time", "slots_in_era", "sync_duration_secs" and "sync_speed_sps" for each era
    eras_list = list(era_details_dict.keys())
    eras_last_idx = len(eras_list) - 1

    for i, era in enumerate(eras_list):
        era_dict = era_details_dict[era]

        if i == eras_last_idx:
            end_sync_time = done_time_str
            last_epoch = actual_epoch
        else:
            next_era = era_details_dict[eras_list[i + 1]]
            end_sync_time = next_era["start_sync_time"]
            last_epoch = int(next_era["start_epoch"]) - 1

        era_dict["last_epoch"] = last_epoch
        era_dict["end_sync_time"] = end_sync_time

        start_epoch = int(era_dict["start_epoch"])
        no_of_epochs_in_era = last_epoch - start_epoch + 1
        era_dict["slots_in_era"] = get_no_of_slots_in_era(
            era_name=era, no_of_epochs_in_era=no_of_epochs_in_era
        )

        start_dt = datetime.datetime.strptime(
            era_dict["start_sync_time"], "%Y-%m-%dT%H:%M:%SZ"
        ).replace(tzinfo=datetime.timezone.utc)
        end_dt = datetime.datetime.strptime(end_sync_time, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=datetime.timezone.utc
        )
        sync_duration_secs = int((end_dt - start_dt).total_seconds())
        era_dict["sync_duration_secs"] = sync_duration_secs

        era_dict["sync_speed_sps"] = int(era_dict["slots_in_era"] / sync_duration_secs)

    # Calculate and add "end_sync_time" and "sync_duration_secs" for each epoch
    epoch_list = list(epoch_details_dict.keys())
    epoch_last_idx = len(epoch_list) - 1

    for i, epoch in enumerate(epoch_list):
        epoch_dict: dict[str, tp.Any] = epoch_details_dict[epoch]

        if i == epoch_last_idx:
            epoch_end_sync_time = done_time_str
        else:
            next_epoch = epoch_details_dict[epoch_list[i + 1]]
            epoch_end_sync_time = next_epoch["start_sync_time"]

        epoch_dict["end_sync_time"] = epoch_end_sync_time

        start_dt = datetime.datetime.strptime(
            epoch_dict["start_sync_time"], "%Y-%m-%dT%H:%M:%SZ"
        ).replace(tzinfo=datetime.timezone.utc)
        end_dt = datetime.datetime.strptime(epoch_end_sync_time, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=datetime.timezone.utc
        )
        sync_duration_secs = int((end_dt - start_dt).total_seconds())
        epoch_dict["sync_duration_secs"] = sync_duration_secs

    return (
        sync_time_seconds,
        last_slot_no,
        latest_chunk_no,
        era_details_dict,
        epoch_details_dict,
    )


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


def copy_cabal_node_exe(repo_dir: pl.Path, dst_dir: pl.Path) -> None:
    node_binary_location_tmp = get_node_executable_path_built_with_cabal(repo_dir=repo_dir)
    assert node_binary_location_tmp is not None  # TODO: refactor
    node_binary_location = node_binary_location_tmp
    shutil.copy2(node_binary_location, dst_dir / "cardano-node")
    helpers.make_executable(path=dst_dir / "cardano-node")


def copy_cabal_cli_exe(repo_dir: pl.Path, dst_dir: pl.Path) -> None:
    cli_binary_location_tmp = get_cli_executable_path_built_with_cabal(repo_dir=repo_dir)
    assert cli_binary_location_tmp is not None  # TODO: refactor
    cli_binary_location = cli_binary_location_tmp
    shutil.copy2(cli_binary_location, dst_dir / "cardano-cli")
    helpers.make_executable(path=dst_dir / "cardano-cli")


def ln_nix_node_from_repo(repo_dir: pl.Path, dst_dir: pl.Path) -> None:
    (dst_dir / "cardano-node").unlink(missing_ok=True)  # Remove existing file if any
    os.symlink(
        repo_dir / "cardano-node-bin" / "bin" / "cardano-node",
        dst_dir / "cardano-node",
    )

    (dst_dir / "cardano-cli").unlink(missing_ok=True)  # Remove existing file if any
    os.symlink(
        repo_dir / "cardano-cli-bin" / "bin" / "cardano-cli",
        dst_dir / "cardano-cli",
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
    bin_directory = pl.Path("bin")

    node_repo = get_node_repo(node_rev=node_rev)
    node_repo_dir = pl.Path(node_repo.git_dir)

    if build_tool == "nix":
        with helpers.temporary_chdir(path=node_repo_dir):
            pl.Path("cardano-node-bin").unlink(missing_ok=True)
            pl.Path("cardano-cli-bin").unlink(missing_ok=True)
            helpers.execute_command("nix build -v .#cardano-node -o cardano-node-bin")
            helpers.execute_command("nix build -v .#cardano-cli -o cardano-cli-bin")
        ln_nix_node_from_repo(repo_dir=node_repo_dir, dst_dir=bin_directory)

    elif build_tool == "cabal":
        cabal_local_file = pl.Path("sync_tests") / "cabal.project.local"
        cli_repo = get_cli_repo(cli_rev="main")
        cli_repo_dir = pl.Path(cli_repo.git_dir)

        # Build cli
        with helpers.temporary_chdir(path=cli_repo_dir):
            shutil.copy2(cabal_local_file, cli_repo_dir)
            LOGGER.debug(f" - listdir cli_repo_dir: {os.listdir(cli_repo_dir)}")
            shutil.rmtree("dist-newstyle", ignore_errors=True)
            for line in fileinput.input("cabal.project", inplace=True):
                LOGGER.debug(line.replace("tests: True", "tests: False"))
            helpers.execute_command("cabal update")
            helpers.execute_command("cabal build cardano-cli")
        copy_cabal_cli_exe(repo_dir=cli_repo_dir, dst_dir=bin_directory)
        gitpython.git_checkout(cli_repo, "cabal.project")

        # Build node
        with helpers.temporary_chdir(path=node_repo_dir):
            shutil.copy2(cabal_local_file, node_repo_dir)
            LOGGER.debug(f" - listdir node_repo_dir: {os.listdir(node_repo_dir)}")
            shutil.rmtree("dist-newstyle", ignore_errors=True)
            for line in fileinput.input("cabal.project", inplace=True):
                LOGGER.debug(line.replace("tests: True", "tests: False"))
            helpers.execute_command("cabal update")
            helpers.execute_command("cabal build cardano-node")
        copy_cabal_node_exe(repo_dir=node_repo_dir, dst_dir=bin_directory)
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

    bin_dir = pl.Path("bin")
    bin_dir.mkdir(exist_ok=True)
    add_to_path(path=bin_dir)

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


def get_node_exit_code(proc: subprocess.Popen) -> int:
    """Get the exit code of a node process if it has finished."""
    if proc.poll() is None:  # None means the process is still running
        return -1

    # Get and report the exit code
    exit_code = proc.returncode
    return exit_code


def run_sync(node_start_arguments: tp.Iterable[str], base_dir: pl.Path, env: str) -> SyncRec | None:
    if "None" in node_start_arguments:
        node_start_arguments = []

    start_sync_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")

    node_proc = None
    logfile = None
    try:
        node_proc, logfile = start_node(
            base_dir=base_dir,
            node_start_arguments=node_start_arguments,
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
