"""Tests for the CI heartbeat sync-progress helpers and status-file writers."""

from __future__ import annotations

import json
import logging
import os
import pathlib as pl
import shutil
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


def test_write_json_to_file_does_not_touch_original_on_write_failure(
    tmp_path: pl.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Proves the write is atomic, not just that no .tmp file is left behind.

    A non-atomic ``open(file_path, "w")`` truncates the real file before
    ``json.dump`` ever runs, so a failure mid-dump destroys the original.
    The temp-file-plus-rename implementation must leave it untouched.
    """
    target = tmp_path / "data.json"
    target.write_text('{"original": true}')

    def _boom(*_args: object, **_kwargs: object) -> None:
        msg = "simulated failure partway through the write"
        raise OSError(msg)

    monkeypatch.setattr(json, "dump", _boom)

    with pytest.raises(OSError, match="simulated failure"):
        helpers.write_json_to_file(target, {"new": True})

    assert json.loads(target.read_text()) == {"original": True}


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


def test_update_json_file_raises_on_unwritable_target(
    tmp_path: pl.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """update_json_file itself only guards a corrupt/missing existing file.

    A write-side failure (e.g. disk full, permission denied) still
    propagates - it's the caller's job to decide whether that should abort,
    same as write_progress_file above does by catching it.

    Uses a monkeypatched failure, not chmod: running as root bypasses a
    permission-based simulation entirely (uid-independent either way).
    """

    def _boom(*_args: object, **_kwargs: object) -> None:
        msg = "simulated permission error"
        raise PermissionError(msg)

    monkeypatch.setattr(helpers, "write_json_to_file", _boom)

    with pytest.raises(PermissionError):
        helpers.update_json_file(tmp_path / "status.json", {"node": {"epoch": 1}})


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


def test_write_progress_file_swallows_write_errors(
    tmp_path: pl.Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Uses a monkeypatched failure, not chmod.

    Running as root (a real possibility on a self-hosted CI runner) bypasses
    a permission-based simulation entirely, which would make this pass for
    the wrong reason - no exception ever raised, the except branch never
    entered - regardless of whether write_progress_file actually catches
    anything.
    """

    def _boom(*_args: object, **_kwargs: object) -> None:
        msg = "simulated disk error"
        raise OSError(msg)

    monkeypatch.setattr(helpers, "write_json_to_file", _boom)

    with caplog.at_level(logging.WARNING):
        # Must not raise: progress display must never abort a sync run.
        node.write_progress_file(workdir=tmp_path, env="preview", tip=_make_tip())

    assert "Failed to write node sync progress file" in caplog.text


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
    """Node and dbsync must come from separate sources, never mirror each other.

    This proves independence at the code level only: that the two keys are
    populated from two distinct function calls and neither leaks into the
    other. It does not, and cannot, prove anything about whether the two
    real underlying formulas (cardano-cli's syncProgress vs a Postgres
    wall-clock ratio over db-sync's own block table) converge on similar
    numbers on a real chain - that was checked separately, against two real
    data sources on a live run, not here.
    """
    config = db_sync.create_db_sync_config(env="preview", workdir=tmp_path, pg_user="test")
    _patch_tip_sources(
        monkeypatch,
        node_tip=_make_tip(era="conway", epoch=500, slot=99_000_000, sync_progress=80.0),
        db_sync_tip=DbSyncTip(epoch_no=10, block_no=200, slot_no=3000),
        db_sync_progress=5.0,
    )

    db_sync._log_sync_progress(config=config, env="preview", start_sync=0.0)

    data = json.loads((tmp_path / "sync_progress_preview.json").read_text())
    assert data["node"] == {
        "era": "conway",
        "epoch": 500,
        "block": 100,
        "slot": 99_000_000,
        "sync_progress": 80.0,
        "updated_at": data["node"]["updated_at"],
    }
    assert data["dbsync"] == {
        "epoch": 10,
        "block": 200,
        "slot": 3000,
        "sync_progress": 5.0,
        "sync_time_h_m_s": data["dbsync"]["sync_time_h_m_s"],
        "updated_at": data["dbsync"]["updated_at"],
    }


def test_log_sync_progress_skips_dbsync_key_before_db_sync_starts(
    tmp_path: pl.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = db_sync.create_db_sync_config(env="preview", workdir=tmp_path, pg_user="test")
    _patch_tip_sources(monkeypatch, node_tip=_make_tip(), db_sync_tip=None, db_sync_progress=None)

    db_sync._log_sync_progress(config=config, env="preview", start_sync=0.0)

    # The node write always happens; assert it actually ran before checking
    # what it left out, or a regression that writes nothing at all would
    # pass this test with no assertion ever executed.
    data = json.loads((tmp_path / "sync_progress_preview.json").read_text())
    assert data["node"]["epoch"] == 5
    assert "dbsync" not in data


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


@pytest.fixture
def _require_jq() -> None:
    """Scoped to the heartbeat tests only.

    Must not gate the 18 pure-Python tests above, or a missing jq silently
    skips the whole file with a green exit code and no signal that the
    sync-progress helpers were never run.
    """
    if shutil.which("jq") is None:
        pytest.skip("jq not available")


@pytest.mark.usefixtures("_require_jq")
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

    assert (
        "progress[node]: 9.44% synced - era=babbage epoch=5 slot=12345 (as of 2026-08-31T00:00:00Z)"
    ) in output


@pytest.mark.usefixtures("_require_jq")
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

    assert (
        "progress[node]: syncProgress unavailable - era=byron epoch=1 slot=100 "
        "(as of 2026-08-31T00:00:00Z)"
    ) in output
    assert "%" not in output.split("progress[node]:")[1].split("\n")[0]


@pytest.mark.usefixtures("_require_jq")
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

    assert (
        "progress[node]: 12.5% synced - era=? epoch=5 slot=12345 (as of 2026-08-31T00:00:00Z)"
    ) in output


@pytest.mark.usefixtures("_require_jq")
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

    assert (
        "progress[node]: 50.0% synced - era=conway epoch=5 slot=12345 (as of 2026-08-31T00:00:00Z)"
    ) in output
    assert "progress[dbsync]:" not in output


@pytest.mark.usefixtures("_require_jq")
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

    assert (
        "progress[node]: 80.0% synced - era=conway epoch=500 slot=99000000 "
        "(as of 2026-08-31T00:00:00Z)"
    ) in output
    assert (
        "progress[dbsync]: 5.0% synced - era=? epoch=10 slot=3000 (as of 2026-08-31T00:00:00Z)"
    ) in output


@pytest.mark.usefixtures("_require_jq")
def test_heartbeat_picks_the_newest_progress_file(tmp_path: pl.Path) -> None:
    """A stale file must lose to the current run's file.

    This can happen when a previous run's workdir is reused, or the
    heartbeat reports old progress.
    """
    (tmp_path / "node_sync.log").touch()

    stale = tmp_path / "sync_progress_mainnet.json"
    _write_progress(
        tmp_path,
        "mainnet",
        {
            "node": {
                "era": "conway",
                "epoch": 9999,
                "slot": 999999999,
                "sync_progress": 100.0,
                "updated_at": "2020-01-01T00:00:00Z",
            }
        },
    )
    os.utime(stale, (1_000_000_000, 1_000_000_000))

    current = tmp_path / "sync_progress_preview.json"
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
    os.utime(current, (2_000_000_000, 2_000_000_000))

    output = _run_heartbeat_tick(tmp_path)

    assert "epoch=5" in output
    assert "epoch=9999" not in output
