"""Pytest fixtures for Cardano node and db-sync sync tests."""

from __future__ import annotations

import dataclasses
import datetime
import json
import logging
import os
import pathlib as pl
import time
import typing as tp
import urllib.error
import urllib.request

import pytest
from _pytest.fixtures import FixtureRequest

from sync_tests.utils import configuration
from sync_tests.utils import db_sync
from sync_tests.utils import helpers
from sync_tests.utils import node
from sync_tests.utils import sync_entries

LOGGER = logging.getLogger(__name__)

NODE_MARKER_TEXT = "SYNC_MARKER_NODE_DONE"
DBSYNC_MARKER_TEXT = "SYNC_MARKER_DBSYNC_DONE"


@dataclasses.dataclass(frozen=True)
class SyncContext:
    """Shared context created once per session."""

    env: str
    workdir: pl.Path
    node_log_path: pl.Path
    db_sync_log_path: pl.Path
    marker_file_path: pl.Path


@dataclasses.dataclass(frozen=True)
class NodeSyncResult:
    """Data returned by the node_synced fixture."""

    node_start_time: str
    node_end_time: str
    node_socket_path: str
    node_tip: dict[str, tp.Any]
    node_marker: tuple[str, str]
    cli_version: str | None = None
    cli_git_rev: str | None = None
    node_logfile_path: pl.Path | None = None
    node_revision: str | None = None
    secs_to_start: int = 0
    sync_time_sec: int = 0
    last_slot_no: int = 0
    latest_chunk_no: int | str = 0
    chain_size_bytes: int = 0
    era_details: dict = dataclasses.field(default_factory=dict)
    epoch_details: dict = dataclasses.field(default_factory=dict)
    full_sync: bool = True


@dataclasses.dataclass(frozen=True)
class DbSyncResult:
    """Data returned by the db_sync_synced fixture."""

    db_start_time: str
    db_end_time: str
    db_tip: dict[str, tp.Any]
    perf_stats_path: pl.Path
    db_marker: tuple[str, str]
    db_sync_version: str | None = None
    db_sync_git_rev: str | None = None
    db_full_sync_time_in_secs: int | None = None
    perf_stats: list[dict] | None = None
    db_sync_revision: str | None = None
    db_start_options: str = ""


def pytest_addoption(parser: tp.Any) -> None:
    """Register custom CLI options consumed by the fixtures."""
    parser.addoption(
        "--environment",
        action="store",
        default="preview",
        help="Cardano environment: preview | preprod | mainnet",
    )
    parser.addoption(
        "--node-revision",
        action="store",
        default=None,
        help="cardano-node tag or branch to build (omit to skip node sync)",
    )
    parser.addoption(
        "--db-sync-revision",
        action="store",
        default=None,
        help="cardano-db-sync tag or branch (omit to skip db-sync)",
    )
    parser.addoption(
        "--db-sync-start-era",
        action="store",
        default="shelley",
        help="Min node era before starting db-sync (default: shelley)",
    )
    parser.addoption(
        "--db-sync-start-options",
        action="store",
        default="",
        help="Extra db-sync start arguments",
    )
    parser.addoption(
        "--node-socket-path",
        action="store",
        default=None,
        help="Path to an existing node socket (skip node sync)",
    )
    parser.addoption(
        "--workdir",
        action="store",
        default=None,
        help="Working directory for logs and artifacts",
    )
    parser.addoption(
        "--snapshot-url",
        action="store",
        default=None,
        help="Snapshot download URL for IOHK snapshot restoration",
    )
    parser.addoption(
        "--run-only-sync-test",
        action="store_true",
        default=False,
        help="Skip snapshot creation/restoration steps",
    )
    parser.addoption(
        "--pg-port",
        action="store",
        default="5432",
        help="PostgreSQL port (default: 5432)",
    )


def _utc_now() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.datetime.now(tz=datetime.timezone.utc).isoformat()


def _validate_configs_base_url(env: str) -> None:
    """Ensure CONFIGS_BASE_URL serves config.json for the selected environment.

    Fails fast with clear error before expensive sync operations begin.

    Args:
        env: Environment name (preview, preprod, mainnet).

    Raises:
        RuntimeError: If config URL is unreachable or returns non-200 status.
    """
    config_url = f"{configuration.CONFIGS_BASE_URL}/{env}/config.json"
    LOGGER.info("Validating config URL: %s", config_url)

    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            with urllib.request.urlopen(config_url, timeout=20) as response:  # nosec B310
                status = response.status
                LOGGER.info("Config URL validation passed (HTTP %s)", status)
                return
        except urllib.error.HTTPError as e:
            msg = (
                f"CONFIGS_BASE_URL does not provide config for env='{env}': {config_url} "
                f"(HTTP {e.code}). "
                f"Current CONFIGS_BASE_URL='{configuration.CONFIGS_BASE_URL}'. "
                f"Use https://book.world.dev.cardano.org/environments for preview/preprod, "
                f"or ensure your candidate URL serves '{env}/config.json'."
            )
            LOGGER.exception(msg)
            raise RuntimeError(msg) from e
        except urllib.error.URLError as e:
            if attempt == max_attempts:
                msg = (
                    f"Network error reaching config URL for env='{env}': {config_url} "
                    f"Reason: {e.reason}"
                )
                LOGGER.exception(msg)
                raise RuntimeError(msg) from e

            backoff_secs = 2 ** (attempt - 1)
            LOGGER.warning(
                "Config URL validation network error on attempt %s/%s: %s. Retrying in %ss.",
                attempt,
                max_attempts,
                e.reason,
                backoff_secs,
            )
            time.sleep(backoff_secs)
        except ValueError as e:
            msg = f"Invalid config URL for env='{env}': {config_url}. Error: {e}"
            LOGGER.exception(msg)
            raise RuntimeError(msg) from e


def _write_marker_to_log(log_path: pl.Path, marker_text: str) -> str:
    """Append a marker line to the log file.

    Args:
        log_path: Path to the log file.
        marker_text: Marker string to write.

    Returns:
        The UTC ISO-8601 timestamp used in the marker line.
    """
    ts = _utc_now()
    line = f"{marker_text} {ts}\n"
    with open(log_path, "a") as fh:
        fh.write(line)
    LOGGER.info("Wrote marker to %s: %s", log_path, line.strip())
    return ts


def _write_marker_to_status(
    marker_file: pl.Path,
    key: str,
    timestamp: str,
    marker_text: str,
) -> None:
    """Upsert a marker entry in the JSON status file.

    Args:
        marker_file: Path to the JSON status file.
        key: Marker key (e.g. ``"node_synced"``).
        timestamp: UTC ISO-8601 timestamp.
        marker_text: Human-readable marker label.
    """
    data: dict[str, tp.Any] = {}
    if marker_file.exists():
        with open(marker_file) as fh:
            data = json.load(fh)
    data[key] = {"timestamp": timestamp, "marker": marker_text}
    with open(marker_file, "w") as fh:
        json.dump(data, fh, indent=2)
    LOGGER.info("Updated marker status file %s [%s]", marker_file, key)


@pytest.fixture(scope="session")
def sync_context(request: FixtureRequest) -> SyncContext:
    """Build shared paths and environment info for the session."""
    env: str = request.config.getoption("--environment")
    LOGGER.info(
        "Starting sync_context setup: env=%s CONFIGS_BASE_URL=%s",
        env,
        configuration.CONFIGS_BASE_URL,
    )
    _validate_configs_base_url(env)
    workdir_opt: str | None = request.config.getoption("--workdir")

    if workdir_opt:
        workdir = pl.Path(workdir_opt).resolve()
    else:
        workdir = (pl.Path.cwd() / "test_workdir").resolve()
    workdir.mkdir(parents=True, exist_ok=True)

    node_log_path = workdir / "node_sync.log"
    db_sync_log_path = workdir / "db_sync.log"
    marker_file_path = workdir / f"sync_markers_{env}.json"

    LOGGER.info("sync_context: env=%s workdir=%s", env, workdir)
    return SyncContext(
        env=env,
        workdir=workdir,
        node_log_path=node_log_path,
        db_sync_log_path=db_sync_log_path,
        marker_file_path=marker_file_path,
    )


@pytest.fixture(scope="session")
def node_synced(
    request: FixtureRequest,
    sync_context: SyncContext,
) -> tp.Generator[NodeSyncResult | None, None, None]:
    """Sync cardano-node and yield timing data; yields None when node sync is skipped."""
    existing_socket: str | None = request.config.getoption(
        "--node-socket-path",
    )
    node_revision: str | None = request.config.getoption(
        "--node-revision",
    )
    env = sync_context.env
    node_started_here = False
    base_dir: pl.Path | None = None
    cli_version: str | None = None
    cli_git_rev: str | None = None

    if not existing_socket and not node_revision:
        LOGGER.info(
            "node_synced: no --node-revision or --node-socket-path; yielding None",
        )
        yield None
        return

    db_sync_revision_opt: str | None = request.config.getoption(
        "--db-sync-revision",
    )
    # With db-sync revision, node sync stops at start_era; otherwise it runs full sync.
    full_sync: bool = db_sync_revision_opt is None

    # Node sync metrics (populated only when this fixture starts the node process).
    sync_secs_to_start: int = 0
    sync_time_sec: int = 0
    last_slot_no: int = 0
    latest_chunk_no: int | str = 0
    chain_size_bytes: int = 0
    era_details: dict = {}
    epoch_details: dict = {}

    if existing_socket:
        socket_path = existing_socket
        if not pl.Path(socket_path).exists():
            msg = f"Provided node socket does not exist: {socket_path}"
            raise FileNotFoundError(msg)
        helpers.export_env_var(
            "CARDANO_NODE_SOCKET_PATH",
            socket_path,
        )
        start_time = _utc_now()
    else:
        if node_revision is None:
            msg = "--node-revision is required when --node-socket-path is not provided"
            raise ValueError(msg)
        start_era: str = request.config.getoption(
            "--db-sync-start-era",
        )
        base_dir = pl.Path.cwd()
        conf_dir = pl.Path.cwd()

        start_time = _utc_now()
        node_result_from_run = sync_entries.run_node_sync(
            env=env,
            node_revision=node_revision,
            node_logfile_path=sync_context.node_log_path,
            base_dir=base_dir,
            conf_dir=conf_dir,
            start_era=start_era,
            full_sync=full_sync,
        )
        node_started_here = True

        cli_version = node_result_from_run.cli_version
        cli_git_rev = node_result_from_run.cli_git_rev
        sync_secs_to_start = node_result_from_run.secs_to_start
        sync_time_sec = node_result_from_run.sync_time_sec
        last_slot_no = node_result_from_run.last_slot_no
        latest_chunk_no = node_result_from_run.latest_chunk_no
        era_details = dict(node_result_from_run.era_details)
        epoch_details = dict(node_result_from_run.epoch_details)

        if full_sync:
            chain_size_bytes = helpers.get_directory_size(
                base_dir / "db",
            )
            LOGGER.info(
                "Chain size on disk: %d bytes",
                chain_size_bytes,
            )

        socket_path = os.environ.get(
            "CARDANO_NODE_SOCKET_PATH",
        ) or str(base_dir / "db" / "node.socket")

    end_time = _utc_now()
    marker_ts = _write_marker_to_log(
        sync_context.node_log_path,
        NODE_MARKER_TEXT,
    )
    _write_marker_to_status(
        sync_context.marker_file_path,
        "node_synced",
        marker_ts,
        NODE_MARKER_TEXT,
    )

    tip = node.get_current_tip(env=env)
    tip_dict = dataclasses.asdict(tip)

    yield NodeSyncResult(
        node_start_time=start_time,
        node_end_time=end_time,
        node_socket_path=socket_path,
        node_tip=tip_dict,
        node_marker=(marker_ts, NODE_MARKER_TEXT),
        cli_version=cli_version,
        cli_git_rev=cli_git_rev,
        node_logfile_path=sync_context.node_log_path,
        node_revision=node_revision,
        secs_to_start=sync_secs_to_start,
        sync_time_sec=sync_time_sec,
        last_slot_no=last_slot_no,
        latest_chunk_no=latest_chunk_no,
        chain_size_bytes=chain_size_bytes,
        era_details=era_details,
        epoch_details=epoch_details,
        full_sync=full_sync,
    )

    if node_started_here:
        LOGGER.info(
            "node_synced teardown: terminating cardano-node",
        )
        helpers.manage_process(
            proc_name="cardano-node",
            action="terminate",
        )
        if base_dir is not None:
            node.rm_node_db_dir(base_dir=base_dir)
    db_sync.stop_monitor(sync_context.workdir)


@pytest.fixture(scope="session")
def db_sync_synced(
    request: FixtureRequest,
    sync_context: SyncContext,
    node_synced: NodeSyncResult | None,
) -> tp.Generator[DbSyncResult, None, None]:
    """Sync cardano-db-sync and yield timing / tip data."""
    db_sync_revision: str | None = request.config.getoption(
        "--db-sync-revision",
    )
    if not db_sync_revision:
        pytest.skip(
            "--db-sync-revision not provided; skipping db-sync",
        )

    if node_synced is None:
        socket_candidates = [
            os.environ.get("CARDANO_NODE_SOCKET_PATH"),
            str(pl.Path.cwd() / "db" / "node.socket"),
            str(sync_context.workdir / "db" / "node.socket"),
        ]
        discovered = next(
            (p for p in socket_candidates if p and pl.Path(p).exists()),
            None,
        )
        if not discovered:
            pytest.skip(
                "No running node: provide --node-revision or --node-socket-path",
            )
        helpers.export_env_var(
            "CARDANO_NODE_SOCKET_PATH",
            discovered,
        )
        LOGGER.info(
            "db_sync_synced: discovered existing socket: %s",
            discovered,
        )

    env = sync_context.env
    db_start_options_raw: str = request.config.getoption("--db-sync-start-options") or ""
    pg_port: str = request.config.getoption("--pg-port") or "5432"

    db_start_options = ""
    if db_start_options_raw.strip():
        db_start_options = " ".join(f"--{opt}" for opt in db_start_options_raw.split())

    config = db_sync.create_db_sync_config(
        env=env,
        workdir=sync_context.workdir,
        pg_port=pg_port,
    )

    config.db_sync_log_file.parent.mkdir(parents=True, exist_ok=True)
    config.db_sync_log_file.write_text("")

    start_time = _utc_now()
    db_result = sync_entries.run_db_sync(
        env=env,
        db_sync_revision=db_sync_revision,
        db_start_options=db_start_options,
        config=config,
        node_logfile_path=sync_context.node_log_path,
    )

    end_time = _utc_now()

    marker_ts = _write_marker_to_log(
        sync_context.db_sync_log_path,
        DBSYNC_MARKER_TEXT,
    )
    _write_marker_to_status(
        sync_context.marker_file_path,
        "db_sync_synced",
        marker_ts,
        DBSYNC_MARKER_TEXT,
    )

    tip_dict = dataclasses.asdict(db_result.db_sync_tip) if db_result.db_sync_tip else {}

    yield DbSyncResult(
        db_start_time=start_time,
        db_end_time=end_time,
        db_tip=tip_dict,
        perf_stats_path=config.perf_stats_file,
        db_marker=(marker_ts, DBSYNC_MARKER_TEXT),
        db_sync_version=db_result.db_sync_version,
        db_sync_git_rev=db_result.db_sync_git_rev,
        db_full_sync_time_in_secs=db_result.db_full_sync_time_in_secs,
        perf_stats=db_result.perf_stats,
        db_sync_revision=db_sync_revision,
        db_start_options=db_start_options,
    )

    LOGGER.info(
        "db_sync_synced teardown: terminating db-sync and postgres",
    )
    helpers.manage_process(
        proc_name="cardano-db-sync",
        action="terminate",
    )
    db_sync.stop_monitor(sync_context.workdir)
    db_sync.stop_postgres(config)
    db_sync.finalize_session_disk_cleanup(config)
