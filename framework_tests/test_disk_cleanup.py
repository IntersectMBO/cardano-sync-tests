"""Tests for db-sync session disk cleanup helpers."""

from __future__ import annotations

import pathlib as pl

import pytest

from sync_tests.utils.db_sync import disk_cleanup
from sync_tests.utils.db_sync.config import create_db_sync_config


def test_rm_postgres_data_dir_removes_data(tmp_path: pl.Path) -> None:
    work = tmp_path / "w"
    work.mkdir()
    config = create_db_sync_config(env="preview", workdir=work)
    data = config.pg_dir / "data"
    data.mkdir(parents=True)
    (data / "placeholder").write_text("x", encoding="utf-8")
    disk_cleanup.rm_postgres_data_dir(config)
    assert not data.exists()


def test_runtime_postgres_socket_dir_uses_tmp_when_path_long(tmp_path: pl.Path) -> None:
    """Paths longer than Postgres Unix socket limit map to /tmp/syncpg-<hash>."""
    deep = tmp_path / ("d" * 90) / "postgres"
    deep.mkdir(parents=True)
    sock = disk_cleanup.runtime_postgres_socket_dir(deep)
    assert sock != deep.resolve()
    assert str(sock).startswith("/tmp/syncpg-")


def test_rm_postgres_data_dir_removes_runtime_socket_dir(tmp_path: pl.Path) -> None:
    """When pg_dir is long, PGDATA and the short /tmp socket directory are removed."""
    work = tmp_path / ("w" * 90)
    work.mkdir()
    pg_dir = work / "postgres"
    pg_dir.mkdir()
    config = create_db_sync_config(env="preview", workdir=work, pg_dir=pg_dir)
    sock = disk_cleanup.runtime_postgres_socket_dir(pg_dir)
    sock.mkdir(parents=True)
    (sock / "stub").write_text("x", encoding="utf-8")
    data = pg_dir / "data"
    data.mkdir(parents=True)
    (data / "placeholder").write_text("y", encoding="utf-8")

    disk_cleanup.rm_postgres_data_dir(config)

    assert not data.exists()
    assert not sock.exists()


@pytest.mark.parametrize(
    ("env_cleanup", "gha", "expected"),
    [
        (None, False, False),
        (None, True, True),
        ("1", False, True),
        ("0", True, False),
        ("yes", False, True),
        ("no", True, False),
    ],
)
def test_should_rm_ledger_state_after_session(
    monkeypatch: pytest.MonkeyPatch,
    env_cleanup: str | None,
    gha: bool,
    expected: bool,
) -> None:
    monkeypatch.delenv("SYNC_TESTS_CLEANUP_LEDGER", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    if env_cleanup is not None:
        monkeypatch.setenv("SYNC_TESTS_CLEANUP_LEDGER", env_cleanup)
    if gha:
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
    assert disk_cleanup.should_rm_ledger_state_after_session() is expected
