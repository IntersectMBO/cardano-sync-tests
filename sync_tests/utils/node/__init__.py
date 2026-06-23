"""Utilities for downloading, configuring, starting, and monitoring cardano-node."""

import dataclasses
import datetime
import fileinput
import functools
import json
import logging
import os
import pathlib as pl
import platform
import re
import shutil
import socket
import subprocess
import time
import typing as tp
import urllib.request

import git

from sync_tests.utils import exceptions
from sync_tests.utils import helpers
from sync_tests.utils.configuration import CONFIGS_BASE_URL
from sync_tests.utils.external import blockfrost
from sync_tests.utils.external import explorer
from sync_tests.utils.external import gitpython
from sync_tests.utils.node import cli

LOGGER = logging.getLogger(__name__)

NODE_LOG_FILE_NAME = "node_sync.log"

TESTNET_ARGS = {
    "mainnet": ("--mainnet",),
    "preview": ("--testnet-magic", "2"),
    "preprod": ("--testnet-magic", "1"),
}


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


@dataclasses.dataclass(frozen=True)
class Tip:
    epoch: int
    block: int
    hash_value: str
    slot: int
    era: str
    sync_progress: float | None


def add_to_path(path: pl.Path) -> None:
    """Add a directory to the system PATH environment variable."""
    os.environ["PATH"] = str(path.absolute()) + os.pathsep + os.environ["PATH"]


def disable_p2p_node_config(config_file: pl.Path) -> None:
    """Disable P2P settings in the node configuration file."""
    helpers.update_json_file(
        file_path=config_file, updates={"EnableP2P": False, "PeerSharing": False}
    )


def disable_genesis_mode(config_file: pl.Path, topology_file: pl.Path) -> None:
    """Disable Genesis mode and switch to Praos mode with bootstrap peers.

    This removes ConsensusMode from config (defaults to Praos) and removes
    peerSnapshotFile from topology (uses bootstrap peers instead).

    Args:
        config_file: Path to the node config.json file.
        topology_file: Path to the node topology.json file.
    """
    helpers.remove_json_keys(file_path=config_file, keys=["ConsensusMode"])
    helpers.remove_json_keys(file_path=topology_file, keys=["peerSnapshotFile"])


def normalize_peer_snapshot(file_path: pl.Path) -> None:
    """Convert 'domain' keys to 'address' in peer snapshot for node compatibility.

    IOG peer-snapshot.json files use 'domain' for DNS-based relays, but
    cardano-node expects 'address' key. This function normalizes the format.

    Args:
        file_path: Path to the peer-snapshot.json file to normalize.
    """
    with open(file_path) as f:
        data = json.load(f)

    modified = False
    for pool in data.get("bigLedgerPools", []):
        for relay in pool.get("relays", []):
            if "domain" in relay and "address" not in relay:
                relay["address"] = relay.pop("domain")
                modified = True

    if modified:
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        LOGGER.info("Normalized peer snapshot: converted 'domain' to 'address'")


def download_config_file(config_slug: str, save_as: pl.Path) -> None:
    url = f"{CONFIGS_BASE_URL}/{config_slug}"
    LOGGER.info("Downloading '%s' and saving as '%s'", url, save_as)
    urllib.request.urlretrieve(url, save_as)


def get_node_config_files(
    env: str, node_topology_type: str, conf_dir: pl.Path, disable_genesis_mode_flag: bool = False
) -> None:
    """Download Cardano node configuration files for the specified environment.

    Genesis mode is the default. Downloaded configs from IOG have Genesis mode enabled.
    Use disable_genesis_mode_flag=True to switch to Praos mode with bootstrap peers.

    Args:
        env: Environment name (preview, preprod, mainnet).
        node_topology_type: Topology type (non-bootstrap-peers, legacy, or default).
        conf_dir: Directory to save configuration files.
        disable_genesis_mode_flag: If True, disable Genesis mode and use Praos mode.
    """
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

    # Always download the environment-specific peer snapshot from IOG
    peer_snapshot_path = conf_dir / "peer-snapshot.json"
    try:
        download_config_file(config_slug=f"{env}/peer-snapshot.json", save_as=peer_snapshot_path)
        LOGGER.info("Downloaded peer snapshot for %s", env)
        # Normalize format: IOG uses 'domain', node expects 'address'
        normalize_peer_snapshot(peer_snapshot_path)
    except Exception:
        LOGGER.warning("peer-snapshot.json not available for %s", env)

    # Genesis mode is the default (configs from IOG have it enabled)
    # Only disable if explicitly requested
    if disable_genesis_mode_flag:
        LOGGER.info("Disabling Genesis mode, switching to Praos mode with bootstrap peers")
        disable_genesis_mode(config_file=config_file_path, topology_file=topology_file_path)

    try:
        download_config_file(
            config_slug=f"{env}/checkpoints.json", save_as=conf_dir / "checkpoints.json"
        )
    except Exception:
        LOGGER.warning("checkpoints.json file not available")


def delete_node_files(node_dir: pl.Path) -> None:
    """Delete all ``cardano-*`` files and directories inside *node_dir*.

    Args:
        node_dir: Directory from which ``cardano-*`` artefacts are removed.
    """
    for p in node_dir.glob("cardano-*"):
        if p.is_dir():
            LOGGER.info("Deleting directory: %s", p)
            shutil.rmtree(p)
        else:
            LOGGER.info("Deleting file: %s", p)
            p.unlink(missing_ok=True)


def _is_ip_address(addr: str) -> bool:
    """Return True if *addr* is a valid IPv4 or IPv6 address."""
    try:
        socket.inet_pton(socket.AF_INET, addr)
    except OSError:
        pass
    else:
        return True
    try:
        socket.inet_pton(socket.AF_INET6, addr)
    except OSError:
        return False
    else:
        return True


def _resolve_relay_domains(pools: list[dict]) -> int:
    """Resolve domain-name relays to IP addresses in-place.

    Version 1 peer snapshots only accept IP addresses in the ``address``
    field.  Version 2 also allows DNS names.  This resolves every
    domain-name relay to its first A-record so v1 nodes can parse them.

    Args:
        pools: The ``bigLedgerPools`` list from the peer snapshot.

    Returns:
        Number of relays that were resolved.
    """
    resolved_count = 0
    for pool in pools:
        new_relays: list[dict] = []
        for relay in pool.get("relays", []):
            addr = relay.get("address", "")
            if _is_ip_address(addr):
                new_relays.append(relay)
                continue
            try:
                ip = socket.gethostbyname(addr)
                relay["address"] = ip
                new_relays.append(relay)
                resolved_count += 1
                LOGGER.debug("Resolved relay %s -> %s", addr, ip)
            except socket.gaierror:
                LOGGER.warning("Could not resolve relay domain %s, dropping", addr)
        pool["relays"] = new_relays
    return resolved_count


def _downgrade_peer_snapshot(snapshot_file: pl.Path) -> None:
    """Downgrade a version 2 peer snapshot to version 1 for older nodes.

    Version 2 allows DNS names in relay ``address`` fields; version 1
    requires IP addresses.  This resolves all domains to IPs and sets
    ``version`` to ``1``.

    Args:
        snapshot_file: Path to the ``peer-snapshot.json`` file.
    """
    if not snapshot_file.exists():
        return

    try:
        with open(snapshot_file) as fh:
            data = json.load(fh)
    except (json.JSONDecodeError, OSError):
        LOGGER.warning("peer-snapshot.json is not valid JSON, skipping downgrade")
        return

    version = data.get("version", 1)
    if version <= 1:
        return

    LOGGER.info(
        "Downgrading peer-snapshot.json from version %d to version 1",
        version,
    )

    pools = data.get("bigLedgerPools", [])
    resolved = _resolve_relay_domains(pools)
    LOGGER.info("Resolved %d domain-name relays to IP addresses", resolved)

    # Drop pools that lost all relays during resolution
    data["bigLedgerPools"] = [p for p in pools if p.get("relays")]

    data["version"] = 1
    with open(snapshot_file, "w") as fh:
        json.dump(data, fh, indent=2)


def _parse_node_major_minor(cli_version: str | None) -> tuple[int, int] | None:
    """Extract (major, minor) from a CLI version string.

    Args:
        cli_version: Version string like ``"cardano-cli 10.6.2 - linux-x86_64 - ghc-9.6"``.

    Returns:
        Tuple of (major, minor) or None if parsing fails.
    """
    if not cli_version:
        return None
    match = re.search(r"(\d+)\.(\d+)", cli_version)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


# Peer snapshot v2 (DNS-based relays) is supported from node 10.5 onward
_MIN_PEER_SNAPSHOT_V2: tp.Final = (10, 5)


def configure_node(
    config_file: pl.Path,
    cli_version: str | None = None,
) -> None:
    """Patch the downloaded node config and topology for sync testing.

    Backward-compatible fixes applied to the IOG-distributed configuration:
    1. Ensures ``EnableP2P`` is set when missing (topology files are P2P format).
    2. Downgrades the peer snapshot to version 1 only for nodes older than 10.5
       that cannot parse v2 DNS-based relays.
    3. Enables resource metrics for the active tracing system.

    Args:
        config_file: Path to the node ``config.json``.
        cli_version: Optional CLI version string used to decide whether
            the peer snapshot needs downgrading.
    """
    LOGGER.info("Configuring node")
    conf_dir = config_file.parent
    topology_file = conf_dir / "topology.json"

    # --- config.json patches ---
    with open(config_file) as fh:
        node_config_json = json.load(fh)

    if "EnableP2P" not in node_config_json:
        node_config_json["EnableP2P"] = True
        LOGGER.info("Set EnableP2P = true (P2P topology format)")

    uses_new_tracing = node_config_json.get("UseTraceDispatcher", False)

    if uses_new_tracing:
        trace_opts = node_config_json.setdefault("TraceOptions", {})
        trace_opts["Resources"] = {"severity": "Info"}
        node_config_json["minSeverity"] = "Info"
        LOGGER.info("Configured new trace-dispatcher for resource monitoring")
    else:
        node_config_json["TraceOptions"] = {}
        node_config_json["UseTraceDispatcher"] = False
        node_config_json["minSeverity"] = "Info"
        options = node_config_json.setdefault("options", {})
        map_backends = options.setdefault("mapBackends", {})
        map_backends["cardano.node.resources"] = ["KatipBK"]
        LOGGER.info("Configured legacy tracing for resource monitoring")

    with open(config_file, "w") as fh:
        json.dump(node_config_json, fh, indent=2)

    # --- peer snapshot downgrade (only for nodes < 10.5) ---
    node_ver = _parse_node_major_minor(cli_version)
    if node_ver and node_ver >= _MIN_PEER_SNAPSHOT_V2:
        LOGGER.info("Node supports peer snapshot v2 natively, skipping downgrade")
    elif topology_file.exists():
        with open(topology_file) as fh:
            topo = json.load(fh)
        snapshot_ref = topo.get("peerSnapshotFile")
        if snapshot_ref:
            _downgrade_peer_snapshot(conf_dir / snapshot_ref)


def set_node_socket_path_env_var(base_dir: pl.Path) -> None:
    socket_path: str | pl.Path
    if "windows" in platform.system().lower():
        socket_path = "\\\\.\\pipe\\cardano-node"
    else:
        # Always set socket path based on base_dir to ensure consistency
        # Don't preserve existing CARDANO_NODE_SOCKET_PATH as it may be from a previous run
        socket_path = (base_dir / "db" / "node.socket").expanduser().absolute()
    os.environ["CARDANO_NODE_SOCKET_PATH"] = str(socket_path)


def get_testnet_args(env: str) -> tp.Iterable[str]:
    try:
        return TESTNET_ARGS[env]
    except KeyError as e:
        msg = f"Unknown environment: {env}"
        raise exceptions.SyncError(msg) from e


def get_current_tip(env: str) -> Tip:
    """Retrieve the current tip of the Cardano node."""
    cardano_cli_path = os.environ.get("CARDANO_CLI_PATH") or "cardano-cli"
    cmd = [cardano_cli_path, "latest", "query", "tip", *get_testnet_args(env=env)]
    output = cli.cli(cli_args=cmd).stdout.decode("utf-8").strip()
    output_json = json.loads(output)

    return Tip(
        epoch=int(output_json.get("epoch", 0)),
        block=int(output_json.get("block", 0)),
        hash_value=output_json.get("hash") or "",
        slot=int(output_json.get("slot", 0)),
        era=output_json.get("era", "").lower(),
        sync_progress=float(output_json.get("syncProgress", 0.0))
        if "syncProgress" in output_json
        else None,
    )


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
            LOGGER.info("%s - Waiting 60s before retrying to get the tip again - %s", now, i)
        time.sleep(60)
    else:
        err_raise = f"Failed to wait for tip: {str_err}"
        raise exceptions.SyncError(err_raise)

    stop_counter = time.perf_counter()
    start_time_seconds = int(stop_counter - start_counter)
    LOGGER.info("It took %s seconds for the QUERY TIP command to be available", start_time_seconds)
    return start_time_seconds


def get_node_version() -> tuple[str, str]:
    try:
        cardano_cli_path = os.environ.get("CARDANO_CLI_PATH") or "cardano-cli"
        output = (
            subprocess.check_output(
                [cardano_cli_path, "--version"],
                stderr=subprocess.STDOUT,
            )
            .decode("utf-8")
            .strip()
        )
        cardano_cli_version = output.split("git rev ")[0].strip()
        cardano_cli_git_rev = output.split("git rev ")[1].strip()
        return str(cardano_cli_version), str(cardano_cli_git_rev)
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        msg = "command '{}' return with error (code {}): {}".format(
            getattr(e, "cmd", "cardano-cli --version"),
            getattr(e, "returncode", 1),
            " ".join(str(getattr(e, "output", str(e))).split()),
        )
        raise exceptions.SyncError(msg) from e


def start_node(
    base_dir: pl.Path,
    node_start_arguments: tp.Iterable[str],
    logfile_path: pl.Path | None = None,
    conf_dir: pl.Path | None = None,
) -> tuple[subprocess.Popen, tp.IO[str]]:
    """Start a cardano-node process.

    Args:
        base_dir: Root directory for the node database and socket.
        node_start_arguments: Extra CLI arguments forwarded to ``cardano-node run``.
        logfile_path: File where stdout/stderr are redirected.  Defaults to
            ``base_dir / NODE_LOG_FILE_NAME``.
        conf_dir: Directory containing ``config.json`` and ``topology.json``.
            Defaults to the current working directory when ``None``.

    Returns:
        A ``(Popen, file)`` tuple for the started process and its log file handle.
    """
    socket_path = os.environ.get("CARDANO_NODE_SOCKET_PATH") or ""
    if not socket_path:
        err = "CARDANO_NODE_SOCKET_PATH environment variable is not set"
        raise exceptions.SyncError(err)
    # Ensure no stale cardano-node process is holding a deleted socket path.
    try:
        existing_proc = helpers.manage_process(proc_name="cardano-node", action="terminate")
        if existing_proc:
            LOGGER.info("Terminated existing cardano-node process before startup")
            time.sleep(2)
    except Exception:
        LOGGER.exception("Failed to terminate existing cardano-node process before startup")
    db_dir = base_dir / "db"
    protocol_magic = db_dir / "protocolMagicId"
    if db_dir.exists() and not protocol_magic.exists():
        try:
            if any(db_dir.iterdir()):
                LOGGER.warning(
                    "Node DB dir is not empty but missing protocolMagicId; "
                    "removing stale contents: %s",
                    db_dir,
                )
                shutil.rmtree(db_dir, ignore_errors=True)
        except OSError:
            LOGGER.exception("Failed to inspect or clean node DB dir before startup")
    socket_path_p = pl.Path(socket_path)
    socket_path_p.parent.mkdir(parents=True, exist_ok=True)
    # Avoid stale socket path from a previous run blocking startup.
    socket_path_p.unlink(missing_ok=True)

    effective_conf_dir = conf_dir if conf_dir is not None else pl.Path.cwd()
    start_args = " ".join(node_start_arguments)
    cmd = (
        "cardano-node run "
        f"--topology {effective_conf_dir / 'topology.json'} "
        f"--database-path {base_dir / 'db'} "
        f"--socket-path {socket_path} "
        f"--config {effective_conf_dir / 'config.json'} "
        "--host-addr 0.0.0.0 "
        "--port 3000 "
        f"{start_args}"
    ).strip()

    LOGGER.info("Starting node with cmd: %s", cmd)
    # Ensure logfile is cleared/truncated before starting node
    if logfile_path is None:
        logfile_path = base_dir / NODE_LOG_FILE_NAME
    logfile_path.parent.mkdir(parents=True, exist_ok=True)
    # Open in "w+" mode to truncate existing file or create new one
    logfile = open(logfile_path, "w+")
    LOGGER.info("Node logfile opened: %s (will write node output here)", logfile_path)

    proc = subprocess.Popen(cmd.split(" "), stdout=logfile, stderr=logfile)
    return proc, logfile


def wait_node_start(
    env: str,
    base_dir: pl.Path,
    timeout_minutes: int = 20,
    logfile_path: pl.Path | None = None,
) -> int:
    """Wait for the Cardano node to start."""
    # when starting from clean state it might take ~30 secs for the cli to work
    # when starting from existing state it might take >10 mins for the cli to work (opening db and
    # replaying the ledger)
    if logfile_path is None:
        logfile_path = base_dir / NODE_LOG_FILE_NAME
    LOGGER.info("Waiting for db folder to be created")
    count = 0
    count_timeout = 299
    while not pl.Path.is_dir(base_dir / "db"):
        time.sleep(1)
        count += 1
        # Show logfile size every 10 seconds to indicate node is writing
        if count % 10 == 0 and logfile_path.exists():
            logfile_size = logfile_path.stat().st_size
            LOGGER.info(
                "Node is writing logs... logfile size: %s bytes"
                " (waiting for db folder, %ss elapsed)",
                logfile_size,
                count,
            )
        if count > count_timeout:
            err_raise = f"Waited {count_timeout} seconds and the DB folder was not created yet"
            raise exceptions.SyncError(err_raise)

    LOGGER.info("DB folder was created after %s seconds", count)
    if logfile_path.exists():
        logfile_size = logfile_path.stat().st_size
        LOGGER.info("Node logfile size: %s bytes (node is running and writing logs)", logfile_size)
    secs_to_start = wait_query_tip_available(env=env, timeout_minutes=timeout_minutes)
    LOGGER.debug(" - listdir current_directory: %s", os.listdir(base_dir))
    LOGGER.debug(" - listdir db: %s", os.listdir(base_dir / "db"))
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


def rm_node_db_dir(base_dir: pl.Path) -> None:
    """Remove the node chain database directory at ``base_dir / 'db'``."""
    db_dir = base_dir / "db"
    if db_dir.exists():
        LOGGER.info("Removing node database directory: %s", db_dir)
        shutil.rmtree(db_dir, ignore_errors=True)


def get_epoch_no_d_zero(env: str) -> int | None:
    """Get the epoch number when d=0."""
    if env == "mainnet":
        return 257
    if env in ("preview", "preprod"):
        return 0
    return None


def get_start_slot_no_d_zero(env: str) -> int | None:
    """Get the start slot number when d=0."""
    if env == "mainnet":
        return 25661009
    if env in ("preview", "preprod"):
        return 0
    return None


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


def find_genesis_json(era_name: str, conf_dir: pl.Path) -> pl.Path:
    """Find genesis JSON file in state dir."""
    potential = [
        *conf_dir.glob(f"*{era_name}*genesis.json"),
        *conf_dir.glob(f"*genesis*{era_name}.json"),
    ]
    if not potential:
        msg = f"The genesis JSON file not found in `{conf_dir}`."
        raise exceptions.SyncError(msg)

    genesis_json = potential[0]
    LOGGER.debug("Using genesis JSON file `%s", genesis_json)
    return genesis_json


def get_genesis(era_name: str, conf_dir: pl.Path) -> dict:
    genesis_file = find_genesis_json(era_name=era_name, conf_dir=conf_dir)
    with open(genesis_file, encoding="utf-8") as in_json:
        genesis_dict: dict = json.load(in_json)
    return genesis_dict


@functools.cache
def get_byron_slot_ln(conf_dir: pl.Path) -> int:
    genesis = get_genesis(era_name="byron", conf_dir=conf_dir)
    return int(genesis["protocolConsts"]["k"]) * 10


@functools.cache
def get_shelley_slot_ln(conf_dir: pl.Path) -> int:
    genesis = get_genesis(era_name="shelley", conf_dir=conf_dir)
    return int(genesis["epochLength"])


def get_no_of_slots_in_era(era_name: str, conf_dir: pl.Path, no_of_epochs_in_era: int) -> int:
    """Get the number of slots in an era."""
    era_name = era_name.lower()
    epoch_length_slots = (
        get_byron_slot_ln(conf_dir=conf_dir)
        if era_name == "byron"
        else get_shelley_slot_ln(conf_dir=conf_dir)
    )
    return int(epoch_length_slots * no_of_epochs_in_era)


def wait_for_shelley_era(
    env: str,
    base_dir: pl.Path,
    timeout_minutes: int = 60,
    min_era: str = "shelley",
    logfile_path: pl.Path | None = None,
) -> None:
    """Wait for the node to reach at least a target era before starting db-sync.

    Historically this waited for Shelley; we now allow a configurable minimum era
    (e.g. "babbage") so tests can start db-sync earlier if desired.

    Args:
        env: Environment name (preview, preprod, mainnet).
        base_dir: Base directory for node files.
        timeout_minutes: Maximum time to wait for the target era.
        min_era: Minimum era name at which to proceed (default: "shelley").
        logfile_path: Node stdout/stderr log file (same as ``start_node``). When
            ``None``, defaults to ``base_dir / NODE_LOG_FILE_NAME`` for backward
            compatibility.

    Raises:
        exceptions.SyncError: If the target era is not reached within timeout.
    """
    LOGGER.info("Waiting for node to reach at least %s era before starting db-sync", min_era)
    start_time = time.perf_counter()
    timeout_seconds = timeout_minutes * 60
    count = 0

    # Simple ordering of eras so we can compare "current >= target"
    era_order: dict[str, int] = {
        "byron": 0,
        "shelley": 1,
        "allegra": 2,
        "mary": 3,
        "alonzo": 4,
        "babbage": 5,
        "conway": 6,
    }
    target_idx = era_order.get(min_era.lower())

    effective_log = logfile_path if logfile_path is not None else base_dir / NODE_LOG_FILE_NAME

    while True:
        tip = get_current_tip(env=env)
        elapsed_minutes = int((time.perf_counter() - start_time) / 60)

        # Log status every 12 iterations (1 minute at 5-second intervals)
        # Show more detailed progress similar to wait_for_node_to_sync()
        if count % 12 == 0:
            logfile_size = effective_log.stat().st_size if effective_log.exists() else 0
            LOGGER.info(
                f"Waiting for target era>={min_era} - current era: {tip.era}, "
                f"epoch: {tip.epoch}, block: {tip.block}, "
                f"slot: {tip.slot}, syncProgress: {tip.sync_progress}, "
                f"elapsed: {elapsed_minutes} minutes, "
                f"node logfile: {logfile_size} bytes"
            )

        # Check if we've reached the target era or later
        current_idx = era_order.get(str(tip.era).lower())
        if target_idx is not None and current_idx is not None and current_idx >= target_idx:
            LOGGER.info(
                f"Node reached {tip.era} era at epoch {tip.epoch}, block {tip.block}. "
                f"Proceeding to start db-sync (min_era={min_era})."
            )
            return

        # Check timeout
        if time.perf_counter() - start_time > timeout_seconds:
            msg = (
                f"Timeout waiting for target era>={min_era} after {timeout_minutes} minutes. "
                f"Current era: {tip.era}, epoch: {tip.epoch}"
            )
            raise exceptions.SyncError(msg)

        time.sleep(5)
        count += 1


def wait_for_node_to_sync(env: str, base_dir: pl.Path) -> tuple:
    """Wait for the Cardano node to start."""
    LOGGER.info("Waiting for the node to sync")
    era_details_dict = {}
    epoch_details_dict = {}

    # Get the initial tip data and calculated slot
    tip = get_current_tip(env=env)
    last_slot_no = get_calculated_slot_no(env=env) if tip.sync_progress is None else -1
    start_sync = time.perf_counter()
    count = 0

    while True:
        # Log status every 60 iterations.
        if count % 60 == 0:
            now_log = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
            LOGGER.info(
                f"{now_log} - actual_era  : {tip.era} "
                f" - actual_epoch: {tip.epoch} "
                f" - actual_block: {tip.block} "
                f" - actual_slot : {tip.slot} "
                f" - syncProgress: {tip.sync_progress}",
            )

        # Use the same current time for both era and epoch updates.
        current_time_str = datetime.datetime.now(tz=datetime.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        # If we see a new era, record its starting details.
        if tip.era not in era_details_dict:
            if env == "mainnet":
                actual_era_start_time = blockfrost.get_epoch_start_datetime(epoch_no=tip.epoch)
            else:
                actual_era_start_time = explorer.get_epoch_start_datetime_from_explorer(
                    env=env, epoch_no=tip.epoch
                )
            era_details_dict[tip.era] = {
                "start_epoch": tip.epoch,
                "start_time": actual_era_start_time,
                "start_sync_time": current_time_str,
            }

        # If we see a new epoch, record its starting sync time.
        if tip.epoch not in epoch_details_dict:
            epoch_details_dict[tip.epoch] = {"start_sync_time": current_time_str}

        # Check termination condition:
        # For nodes reporting sync progress, we wait until progress reaches 100.
        if tip.sync_progress is not None and tip.sync_progress >= 100:
            break
        # Otherwise (for nodes without sync progress) wait until the slot number passes
        # the calculated value.
        if tip.sync_progress is None and tip.slot > last_slot_no:
            break

        time.sleep(5)
        count += 1
        tip = get_current_tip(env=env)

    done_time_str = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    end_sync = time.perf_counter()
    sync_time_seconds = int(end_sync - start_sync)
    LOGGER.info("sync_time_seconds: %s", sync_time_seconds)

    chunk_files = sorted((base_dir / "db" / "immutable").iterdir(), key=lambda f: f.stat().st_mtime)
    latest_chunk_no = chunk_files[-1].stem
    LOGGER.info("Sync done!; latest_chunk_no: %s", latest_chunk_no)

    # Add "end_sync_time", "slots_in_era", "sync_duration_secs" and "sync_speed_sps" for each era
    eras_list = list(era_details_dict.keys())
    eras_last_idx = len(eras_list) - 1

    for i, era in enumerate(eras_list):
        era_dict: dict[str, tp.Any] = era_details_dict[era]

        if i == eras_last_idx:
            end_sync_time = done_time_str
            last_epoch = tip.epoch
        else:
            next_era: dict[str, tp.Any] = era_details_dict[eras_list[i + 1]]
            end_sync_time = next_era["start_sync_time"]
            last_epoch = int(next_era["start_epoch"]) - 1

        era_dict["last_epoch"] = last_epoch
        era_dict["end_sync_time"] = end_sync_time

        start_epoch = int(era_dict["start_epoch"])
        no_of_epochs_in_era = last_epoch - start_epoch + 1
        era_dict["slots_in_era"] = get_no_of_slots_in_era(
            era_name=era, conf_dir=base_dir, no_of_epochs_in_era=no_of_epochs_in_era
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


def get_node_executable_path_built_with_cabal(repo_dir: pl.Path) -> pl.Path:
    for f in get_cabal_build_files(repo_dir=repo_dir):
        if (
            "x" in f.parts
            and "cardano-node" in f.parts
            and "build" in f.parts
            and "cardano-node-tmp" not in f.name
            and "autogen" not in f.name
        ):
            return f
    err = "Could not find the cardano-node executable built with cabal"
    raise exceptions.SyncError(err)


def get_cli_executable_path_built_with_cabal(repo_dir: pl.Path) -> pl.Path:
    for f in get_cabal_build_files(repo_dir=repo_dir):
        if (
            "x" in f.parts
            and "cardano-cli" in f.parts
            and "build" in f.parts
            and "cardano-cli-tmp" not in f.name
            and "autogen" not in f.name
        ):
            return f
    err = "Could not find the cardano-cli executable built with cabal"
    raise exceptions.SyncError(err)


def copy_cabal_node_exe(repo_dir: pl.Path, dst_dir: pl.Path) -> None:
    node_binary_location_tmp = get_node_executable_path_built_with_cabal(repo_dir=repo_dir)
    node_binary_location = node_binary_location_tmp
    shutil.copy2(node_binary_location, dst_dir / "cardano-node")
    helpers.make_executable(path=dst_dir / "cardano-node")


def copy_cabal_cli_exe(repo_dir: pl.Path, dst_dir: pl.Path) -> None:
    cli_binary_location_tmp = get_cli_executable_path_built_with_cabal(repo_dir=repo_dir)
    cli_binary_location = cli_binary_location_tmp
    shutil.copy2(cli_binary_location, dst_dir / "cardano-cli")
    helpers.make_executable(path=dst_dir / "cardano-cli")


def copy_nix_node_from_repo(repo_dir: pl.Path, dst_dir: pl.Path) -> None:
    """Copy nix-built binaries instead of symlinking to avoid nix GC breaking the store paths."""
    (dst_dir / "cardano-node").unlink(missing_ok=True)
    shutil.copy2(
        repo_dir / "cardano-node-bin" / "bin" / "cardano-node",
        dst_dir / "cardano-node",
    )
    helpers.make_executable(path=dst_dir / "cardano-node")

    (dst_dir / "cardano-cli").unlink(missing_ok=True)
    shutil.copy2(
        repo_dir / "cardano-cli-bin" / "bin" / "cardano-cli",
        dst_dir / "cardano-cli",
    )
    helpers.make_executable(path=dst_dir / "cardano-cli")


def get_node_repo(node_rev: str, base_dir: pl.Path) -> git.Repo:
    node_repo_name = "cardano-node"
    node_repo_dir = base_dir / "cardano_node_dir"

    if node_repo_dir.is_dir():
        repo = git.Repo(path=node_repo_dir)
        gitpython.git_checkout(repo, node_rev)
    else:
        repo = gitpython.git_clone_iohk_repo(node_repo_name, node_repo_dir, node_rev)

    return repo


def get_cli_repo(cli_rev: str, base_dir: pl.Path) -> git.Repo:
    node_repo_name = "cardano-cli"
    cli_repo_dir = base_dir / "cardano_cli_dir"

    if cli_repo_dir.is_dir():
        repo = git.Repo(path=cli_repo_dir)
        gitpython.git_checkout(repo, cli_rev)
    else:
        repo = gitpython.git_clone_iohk_repo(node_repo_name, cli_repo_dir, cli_rev)

    return repo


def get_node_files(node_rev: str, base_dir: pl.Path, build_tool: str = "nix") -> git.Repo:
    bin_directory = base_dir / "bin"

    node_repo = get_node_repo(node_rev=node_rev, base_dir=base_dir)
    # Use working tree (repo root), not .git dir: nix build outputs live in the repo root.
    node_repo_dir = pl.Path(node_repo.working_tree_dir or node_repo.git_dir)

    if build_tool == "nix":
        (node_repo_dir / "cardano-node-bin").unlink(missing_ok=True)
        (node_repo_dir / "cardano-cli-bin").unlink(missing_ok=True)
        helpers.execute_command(
            "nix build -v --accept-flake-config --print-build-logs .#cardano-node "
            "-o cardano-node-bin",
            cwd=node_repo_dir,
        )
        helpers.execute_command(
            "nix build -v --accept-flake-config --print-build-logs .#cardano-cli "
            "-o cardano-cli-bin",
            cwd=node_repo_dir,
        )
        copy_nix_node_from_repo(repo_dir=node_repo_dir, dst_dir=bin_directory)

    elif build_tool == "cabal":
        repo_root = pl.Path(__file__).parent.parent.parent.parent
        cabal_local_file = repo_root / "sync_tests" / "cabal.project.local"
        cli_repo = get_cli_repo(cli_rev="main", base_dir=base_dir)
        cli_repo_dir = pl.Path(cli_repo.working_tree_dir or cli_repo.git_dir)

        # Build cli
        shutil.copy2(cabal_local_file, cli_repo_dir)
        LOGGER.debug(" - listdir cli_repo_dir: %s", os.listdir(cli_repo_dir))
        shutil.rmtree(cli_repo_dir / "dist-newstyle", ignore_errors=True)
        for line in fileinput.input(str(cli_repo_dir / "cabal.project"), inplace=True):
            LOGGER.debug(line.replace("tests: True", "tests: False"))
        helpers.execute_command("cabal update", cwd=cli_repo_dir)
        helpers.execute_command("cabal build cardano-cli", cwd=cli_repo_dir)
        copy_cabal_cli_exe(repo_dir=cli_repo_dir, dst_dir=bin_directory)
        gitpython.git_checkout(cli_repo, "cabal.project")

        # Build node
        shutil.copy2(cabal_local_file, node_repo_dir)
        LOGGER.debug(" - listdir node_repo_dir: %s", os.listdir(node_repo_dir))
        shutil.rmtree(node_repo_dir / "dist-newstyle", ignore_errors=True)
        for line in fileinput.input(str(node_repo_dir / "cabal.project"), inplace=True):
            LOGGER.debug(line.replace("tests: True", "tests: False"))
        helpers.execute_command("cabal update", cwd=node_repo_dir)
        helpers.execute_command("cabal build cardano-node", cwd=node_repo_dir)
        copy_cabal_node_exe(repo_dir=node_repo_dir, dst_dir=bin_directory)
        gitpython.git_checkout(node_repo, "cabal.project")

    return node_repo


def config_sync(
    env: str,
    base_dir: pl.Path,
    node_rev: str,
    node_topology_type: str,
    disable_genesis_mode: bool = False,
) -> None:
    """Configure the node for syncing.

    Args:
        env: Environment name (preview, preprod, mainnet).
        base_dir: Base directory for node files.
        node_rev: Node revision/tag to use.
        node_topology_type: Topology type.
        disable_genesis_mode: If True, disable Genesis mode and use Praos mode.
    """
    LOGGER.info("Get the cardano-node and cardano-cli files")
    start_build_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")

    bin_dir = base_dir / "bin"
    bin_dir.mkdir(exist_ok=True)
    add_to_path(path=bin_dir)

    platform_system = platform.system().lower()
    if "windows" in platform_system:
        get_node_files(node_rev=node_rev, base_dir=base_dir, build_tool="cabal")
    else:
        get_node_files(node_rev=node_rev, base_dir=base_dir)

    end_build_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%Y %H:%M:%S")
    LOGGER.info("  - start_build_time: %s", start_build_time)
    LOGGER.info("  - end_build_time: %s", end_build_time)

    rm_node_config_files(conf_dir=base_dir)
    get_node_config_files(
        env=env,
        node_topology_type=node_topology_type,
        conf_dir=base_dir,
        disable_genesis_mode_flag=disable_genesis_mode,
    )

    configure_node(config_file=base_dir / "config.json")
    if env == "mainnet" and node_topology_type == "legacy":
        disable_p2p_node_config(config_file=base_dir / "config.json")


def get_node_exit_code(proc: subprocess.Popen) -> int:
    """Get the exit code of a node process if it has finished."""
    if proc.poll() is None:  # None means the process is still running
        return -1

    # Get and report the exit code
    exit_code = proc.returncode
    return exit_code


def run_sync(node_start_arguments: tp.Iterable[str], base_dir: pl.Path, env: str) -> SyncRec | None:
    start_sync_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%d/%m/%YT%H:%M:%S")

    node_proc = None
    logfile = None
    try:
        node_proc, logfile = start_node(
            base_dir=base_dir,
            node_start_arguments=node_start_arguments,
        )
        secs_to_start = wait_node_start(env=env, base_dir=base_dir, timeout_minutes=10)
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
                LOGGER.error("Node exited unexpectedly with code: %s", node_status)
            else:
                exit_code = stop_node(proc=node_proc)
                LOGGER.warning("Node stopped with exit code: %s", exit_code)
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
