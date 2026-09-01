"""Tests for the CI heartbeat sync-progress helpers and status-file writers."""

from __future__ import annotations

import json
import os
import pathlib as pl
import subprocess

import pytest

from sync_tests.utils import db_sync
from sync_tests.utils import helpers
from sync_tests.utils import node
from sync_tests.utils.db_sync.config import DbSyncTip

_REPO_ROOT = pl.Path(__file__).resolve().parents[1]
_HEARTBEAT_SH = _REPO_ROOT / "sync_tests" / "scripts" / "heartbeat.sh"


def _make_tip(
    era: str = "babbage",
    epoch: int = 5,
    block: int = 100,
    slot: int = 12345,
    sync_progress: float | None = 42.5,
) -> node.Tip:
    return node.Tip(
        epoch=epoch,
        block=block,
        hash_value="deadbeef",
        slot=slot,
        era=era,
        sync_progress=sync_progress,
    )


# --- helpers.write_json_to_file -----------------------------------------------


def test_write_json_to_file_writes_correct_content(tmp_path: pl.Path) -> None:
    target = tmp_path / "data.json"
    helpers.write_json_to_file(target, {"a": 1})

    assert json.loads(target.read_text()) == {"a": 1}


def test_write_json_to_file_leaves_no_temp_file_behind(tmp_path: pl.Path) -> None:
    target = tmp_path / "data.json"
    helpers.write_json_to_file(target, {"a": 1})

    assert sorted(p.name for p in tmp_path.iterdir()) == ["data.json"]


def test_write_json_to_file_creates_parent_dirs(tmp_path: pl.Path) -> None:
    target = tmp_path / "nested" / "dir" / "data.json"
    helpers.write_json_to_file(target, {"a": 1})

    assert json.loads(target.read_text()) == {"a": 1}


# --- helpers.update_json_file --------------------------------------------------


def test_update_json_file_creates_missing_file(tmp_path: pl.Path) -> None:
    target = tmp_path / "status.json"
    helpers.update_json_file(target, {"node": {"epoch": 1}})

    assert json.loads(target.read_text()) == {"node": {"epoch": 1}}


def test_update_json_file_preserves_other_keys(tmp_path: pl.Path) -> None:
    target = tmp_path / "status.json"
    helpers.update_json_file(target, {"node": {"epoch": 1}})
    helpers.update_json_file(target, {"dbsync": {"epoch": 2}})

    assert json.loads(target.read_text()) == {
        "node": {"epoch": 1},
        "dbsync": {"epoch": 2},
    }


def test_update_json_file_overwrites_same_key(tmp_path: pl.Path) -> None:
    target = tmp_path / "status.json"
    helpers.update_json_file(target, {"node": {"epoch": 1}})
    helpers.update_json_file(target, {"node": {"epoch": 2}})

    assert json.loads(target.read_text()) == {"node": {"epoch": 2}}


@pytest.mark.parametrize("existing_content", ["", "{", '{"node": "unterm'])
def test_update_json_file_recovers_from_corrupt_existing_file(
    tmp_path: pl.Path, existing_content: str
) -> None:
    target = tmp_path / "status.json"
    target.write_text(existing_content)

    helpers.update_json_file(target, {"node": {"epoch": 1}})

    assert json.loads(target.read_text()) == {"node": {"epoch": 1}}


def test_update_json_file_ignores_non_dict_existing_content(tmp_path: pl.Path) -> None:
    target = tmp_path / "status.json"
    target.write_text(json.dumps(["not", "a", "dict"]))

    helpers.update_json_file(target, {"node": {"epoch": 1}})

    assert json.loads(target.read_text()) == {"node": {"epoch": 1}}


def test_update_json_file_raises_on_unwritable_target(tmp_path: pl.Path) -> None:
    """update_json_file itself only guards a corrupt/missing existing file.

    A write-side failure (e.g. disk full, permission denied) still
    propagates - it's the caller's job to decide whether that should abort,
    same as write_progress_file below does by catching it.
    """
    bad_dir = tmp_path / "nowrite"
    bad_dir.mkdir()
    os.chmod(bad_dir, 0o500)
    try:
        with pytest.raises(PermissionError):
            helpers.update_json_file(bad_dir / "status.json", {"node": {"epoch": 1}})
    finally:
        os.chmod(bad_dir, 0o700)


# --- node.write_progress_file --------------------------------------------------


def test_write_progress_file_writes_node_key(tmp_path: pl.Path) -> None:
    node.write_progress_file(workdir=tmp_path, env="preview", tip=_make_tip())

    data = json.loads((tmp_path / "sync_progress_preview.json").read_text())
    assert data["node"] == {
        "era": "babbage",
        "epoch": 5,
        "block": 100,
        "slot": 12345,
        "sync_progress": 42.5,
        "updated_at": data["node"]["updated_at"],
    }
    assert data["node"]["updated_at"].endswith("Z")


def test_write_progress_file_none_workdir_is_a_noop(tmp_path: pl.Path) -> None:
    node.write_progress_file(workdir=None, env="preview", tip=_make_tip())

    assert list(tmp_path.iterdir()) == []


def test_write_progress_file_swallows_write_errors(tmp_path: pl.Path) -> None:
    bad_dir = tmp_path / "nowrite"
    bad_dir.mkdir()
    os.chmod(bad_dir, 0o500)
    try:
        # Must not raise: progress display must never abort a sync run.
        node.write_progress_file(workdir=bad_dir, env="preview", tip=_make_tip())
    finally:
        os.chmod(bad_dir, 0o700)


def test_write_progress_file_preserves_dbsync_key(tmp_path: pl.Path) -> None:
    helpers.write_json_to_file(tmp_path / "sync_progress_preview.json", {"dbsync": {"epoch": 99}})

    node.write_progress_file(workdir=tmp_path, env="preview", tip=_make_tip())

    data = json.loads((tmp_path / "sync_progress_preview.json").read_text())
    assert data["dbsync"] == {"epoch": 99}
    assert data["node"]["epoch"] == 5


# --- db_sync's dbsync-key write (via _log_sync_progress) ----------------------


def _patch_tip_sources(
    monkeypatch: pytest.MonkeyPatch,
    *,
    node_tip: node.Tip | None,
    db_sync_tip: DbSyncTip | None,
    db_sync_progress: float | None,
) -> None:
    def fake_get_current_tip(env: str) -> node.Tip:  # noqa: ARG001
        if node_tip is None:
            msg = "node tip unavailable"
            raise RuntimeError(msg)
        return node_tip

    monkeypatch.setattr(db_sync.node, "get_current_tip", fake_get_current_tip)
    monkeypatch.setattr(db_sync.postgres, "get_db_sync_tip", lambda _config: db_sync_tip)
    monkeypatch.setattr(db_sync.postgres, "get_db_sync_progress", lambda _config: db_sync_progress)


def test_log_sync_progress_writes_independent_dbsync_key(
    tmp_path: pl.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Node and dbsync must come from separate sources, never mirror each other."""
    config = db_sync.create_db_sync_config(env="preview", workdir=tmp_path, pg_user="test")
    _patch_tip_sources(
        monkeypatch,
        node_tip=_make_tip(era="conway", epoch=500, slot=99_000_000, sync_progress=80.0),
        db_sync_tip=DbSyncTip(epoch_no=10, block_no=200, slot_no=3000),
        db_sync_progress=5.0,
    )

    db_sync._log_sync_progress(config=config, env="preview", start_sync=0.0)

    data = json.loads((tmp_path / "sync_progress_preview.json").read_text())
    assert data["node"]["epoch"] == 500
    assert data["node"]["slot"] == 99_000_000
    assert data["node"]["sync_progress"] == 80.0
    assert data["dbsync"]["epoch"] == 10
    assert data["dbsync"]["slot"] == 3000
    assert data["dbsync"]["sync_progress"] == 5.0


def test_log_sync_progress_skips_dbsync_key_before_db_sync_starts(
    tmp_path: pl.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = db_sync.create_db_sync_config(env="preview", workdir=tmp_path, pg_user="test")
    _patch_tip_sources(monkeypatch, node_tip=_make_tip(), db_sync_tip=None, db_sync_progress=None)

    db_sync._log_sync_progress(config=config, env="preview", start_sync=0.0)

    progress_file = tmp_path / "sync_progress_preview.json"
    if progress_file.exists():
        assert "dbsync" not in json.loads(progress_file.read_text())


def test_log_sync_progress_survives_node_tip_failure(
    tmp_path: pl.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = db_sync.create_db_sync_config(env="preview", workdir=tmp_path, pg_user="test")
    _patch_tip_sources(
        monkeypatch,
        node_tip=None,
        db_sync_tip=DbSyncTip(epoch_no=10, block_no=200, slot_no=3000),
        db_sync_progress=5.0,
    )

    db_sync._log_sync_progress(config=config, env="preview", start_sync=0.0)

    data = json.loads((tmp_path / "sync_progress_preview.json").read_text())
    assert "node" not in data
    assert data["dbsync"]["epoch"] == 10


# --- heartbeat.sh ---------------------------------------------------------------


def _run_heartbeat_tick(workdir: pl.Path, mode: str = "node-only", timeout: float = 3.0) -> str:
    """Run one heartbeat.sh tick and return its stdout.

    heartbeat.sh runs its first tick immediately, then loops forever on
    ``sleep $INTERVAL``. Timing out mid-sleep is the intended way to capture
    exactly one tick's output.
    """
    env = dict(os.environ, SYNC_TESTS_HEARTBEAT_WORKDIR=str(workdir))
    try:
        result = subprocess.run(
            ["bash", str(_HEARTBEAT_SH), mode],
            env=env,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
        return result.stdout.decode("utf-8")
    except subprocess.TimeoutExpired as exc:
        return (exc.stdout or b"").decode("utf-8")


def _write_progress(workdir: pl.Path, env: str, payload: dict) -> None:
    helpers.write_json_to_file(workdir / f"sync_progress_{env}.json", payload)


@pytest.fixture(autouse=True)
def _require_jq() -> None:
    if subprocess.run(["which", "jq"], capture_output=True, check=False).returncode != 0:
        pytest.skip("jq not available")


def test_heartbeat_prints_percent_when_available(tmp_path: pl.Path) -> None:
    (tmp_path / "node_sync.log").touch()
    _write_progress(
        tmp_path,
        "preview",
        {
            "node": {
                "era": "babbage",
                "epoch": 5,
                "slot": 12345,
                "sync_progress": 9.44,
                "updated_at": "2026-08-31T00:00:00Z",
            }
        },
    )

    output = _run_heartbeat_tick(tmp_path)

    assert "progress[node]: 9.44% synced - era=babbage epoch=5 slot=12345" in output


def test_heartbeat_shows_era_epoch_slot_when_percent_missing(tmp_path: pl.Path) -> None:
    (tmp_path / "node_sync.log").touch()
    _write_progress(
        tmp_path,
        "preview",
        {
            "node": {
                "era": "byron",
                "epoch": 1,
                "slot": 100,
                "sync_progress": None,
                "updated_at": "2026-08-31T00:00:00Z",
            }
        },
    )

    output = _run_heartbeat_tick(tmp_path)

    assert "progress[node]: syncProgress unavailable - era=byron epoch=1 slot=100" in output
    assert "%" not in output.split("progress[node]:")[1].split("\n")[0]


def test_heartbeat_shows_placeholder_for_empty_era(tmp_path: pl.Path) -> None:
    (tmp_path / "node_sync.log").touch()
    _write_progress(
        tmp_path,
        "preview",
        {
            "node": {
                "era": "",
                "epoch": 5,
                "slot": 12345,
                "sync_progress": 12.5,
                "updated_at": "2026-08-31T00:00:00Z",
            }
        },
    )

    output = _run_heartbeat_tick(tmp_path)

    assert "era=?" in output


def test_heartbeat_prints_nothing_for_key_not_yet_present(tmp_path: pl.Path) -> None:
    (tmp_path / "node_sync.log").touch()
    (tmp_path / "db_sync.log").touch()
    _write_progress(
        tmp_path,
        "preview",
        {
            "node": {
                "era": "conway",
                "epoch": 5,
                "slot": 12345,
                "sync_progress": 50.0,
                "updated_at": "2026-08-31T00:00:00Z",
            }
        },
    )

    output = _run_heartbeat_tick(tmp_path, mode="combined")

    assert "progress[node]:" in output
    assert "progress[dbsync]:" not in output


def test_heartbeat_prints_both_keys_independently(tmp_path: pl.Path) -> None:
    (tmp_path / "node_sync.log").touch()
    (tmp_path / "db_sync.log").touch()
    _write_progress(
        tmp_path,
        "preview",
        {
            "node": {
                "era": "conway",
                "epoch": 500,
                "slot": 99_000_000,
                "sync_progress": 80.0,
                "updated_at": "2026-08-31T00:00:00Z",
            },
            "dbsync": {
                "epoch": 10,
                "slot": 3000,
                "sync_progress": 5.0,
                "updated_at": "2026-08-31T00:00:00Z",
            },
        },
    )

    output = _run_heartbeat_tick(tmp_path, mode="combined")

    assert "progress[node]: 80.0% synced" in output
    assert "epoch=500" in output
    assert "progress[dbsync]: 5.0% synced" in output
    assert "epoch=10" in output
