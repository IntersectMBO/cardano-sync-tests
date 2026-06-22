"""Remove Postgres PGDATA and optionally db-sync ledger-state after a session."""

from __future__ import annotations

import hashlib
import logging
import os
import pathlib as pl
import shutil

from sync_tests.utils.db_sync.config import DbSyncConfig
from sync_tests.utils.path_utils import get_db_sync_dir

LOGGER = logging.getLogger(__name__)

# Must match ``postgres-start.sh``: room for "/.s.PGSQL.<port>" under Unix max ~107.
_MAX_UNIX_SOCKET_PARENT_LEN = 107 - 22


def runtime_postgres_socket_dir(pg_dir: pl.Path) -> pl.Path:
    """Directory used for Postgres ``-k`` (Unix sockets), matching ``postgres-start.sh``.

    When ``pg_dir`` is too long for a socket file path, returns
    ``/tmp/syncpg-<sha256[:16]>`` so the server can bind; otherwise returns
    ``pg_dir`` resolved.
    """
    resolved = pg_dir.resolve()
    if len(str(resolved)) <= _MAX_UNIX_SOCKET_PARENT_LEN:
        return resolved
    digest = hashlib.sha256(str(resolved).encode()).hexdigest()[:16]
    return pl.Path("/tmp") / f"syncpg-{digest}"


def rm_postgres_data_dir(config: DbSyncConfig) -> None:
    """Remove the Postgres cluster data directory for this config's ``pg_dir``.

    Call only after :func:`sync_tests.utils.db_sync.postgres.stop_postgres` so
    the server is not running.

    Args:
        config: Db-sync config whose ``pg_dir`` scoped this Postgres instance.
    """
    sock_dir = runtime_postgres_socket_dir(config.pg_dir)
    if sock_dir != config.pg_dir.resolve() and sock_dir.exists():
        LOGGER.info("Removing postgres runtime socket directory: %s", sock_dir)
        shutil.rmtree(sock_dir, ignore_errors=True)

    data_dir = config.pg_dir / "data"
    if data_dir.exists():
        LOGGER.info("Removing postgres PGDATA directory: %s", data_dir)
        shutil.rmtree(data_dir, ignore_errors=True)


def should_rm_ledger_state_after_session() -> bool:
    """Whether to delete ``ledger-state/<env>`` after the pytest session.

    Controlled by ``SYNC_TESTS_CLEANUP_LEDGER``: ``0``/``false``/``no`` forces
    off; ``1``/``true``/``yes`` forces on; if unset, defaults to on when
    ``GITHUB_ACTIONS`` is set (CI), off otherwise.

    Returns:
        ``True`` if ledger state directory should be removed.
    """
    explicit = os.environ.get("SYNC_TESTS_CLEANUP_LEDGER", "").strip().lower()
    if explicit in ("0", "false", "no"):
        return False
    if explicit in ("1", "true", "yes"):
        return True
    return bool(os.environ.get("GITHUB_ACTIONS"))


def rm_db_sync_ledger_state_dir(*, env: str, db_sync_dir: pl.Path | None = None) -> None:
    """Remove ``ledger-state/<env>`` under the cardano-db-sync checkout.

    Args:
        env: Network name (e.g. ``mainnet``) matching the state directory segment.
        db_sync_dir: Root of the cardano-db-sync repo clone; defaults to
            :func:`sync_tests.utils.path_utils.get_db_sync_dir`.
    """
    root = db_sync_dir if db_sync_dir is not None else get_db_sync_dir()
    ledger = root / "ledger-state" / env
    if ledger.exists():
        LOGGER.info("Removing db-sync ledger state directory: %s", ledger)
        shutil.rmtree(ledger, ignore_errors=True)


def finalize_session_disk_cleanup(config: DbSyncConfig) -> None:
    """Remove PGDATA for ``config``; optionally remove db-sync ledger state."""
    rm_postgres_data_dir(config)
    if should_rm_ledger_state_after_session():
        rm_db_sync_ledger_state_dir(env=config.env)
