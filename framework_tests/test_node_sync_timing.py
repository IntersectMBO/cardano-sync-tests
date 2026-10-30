"""Regression tests for syncing before the first successful tip query."""

import datetime
import json
import pathlib as pl
from unittest.mock import Mock

import pytest

from sync_tests.utils import node
from sync_tests.utils import sync_entries

_START = datetime.datetime(2026, 10, 28, 12, 0, tzinfo=datetime.UTC)


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> dict[str, float]:
    """Advance wall and monotonic time together without sleeping."""
    state = {"elapsed": 0.0}

    class FakeDatetime(datetime.datetime):
        @classmethod
        def now(cls, tz: datetime.tzinfo | None = None) -> datetime.datetime:
            return (_START + datetime.timedelta(seconds=state["elapsed"])).astimezone(tz)

    def advance(seconds: float) -> None:
        state["elapsed"] += seconds

    monkeypatch.setattr(node.datetime, "datetime", FakeDatetime)
    monkeypatch.setattr(node.time, "perf_counter", lambda: 1000.0 + state["elapsed"])
    monkeypatch.setattr(node.time, "sleep", advance)
    return state


def _tip(era: str, epoch: int, slot: int, progress: float) -> node.Tip:
    return node.Tip(
        era=era, epoch=epoch, slot=slot, block=slot, hash_value="abc", sync_progress=progress
    )


@pytest.mark.parametrize("clean_start", [True, False])
def test_initial_byron_timing(
    tmp_path: pl.Path,
    monkeypatch: pytest.MonkeyPatch,
    clock: dict[str, float],
    clean_start: bool,
) -> None:
    """A clean launch includes the epochs and time hidden by CLI readiness."""
    immutable = tmp_path / "db" / "immutable"
    immutable.mkdir(parents=True)
    (immutable / "00000.chunk").touch()
    (tmp_path / "byron-genesis.json").write_text(json.dumps({"protocolConsts": {"k": 2160}}))
    (tmp_path / "shelley-genesis.json").write_text(json.dumps({"epochLength": 432000}))
    tips = iter(
        [
            _tip("byron", 4, 102085, 1.0),
            _tip("shelley", 208, 4492800, 50.0),
            _tip("shelley", 209, 4924800, 100.0),
        ]
    )
    monkeypatch.setattr(node, "get_current_tip", lambda **_kwargs: next(tips))
    lookup = Mock(side_effect=lambda epoch_no: f"epoch-{epoch_no}")
    monkeypatch.setattr(node.blockfrost, "get_epoch_start_datetime", lookup)
    monkeypatch.setattr(node, "refresh_and_write_progress", Mock(return_value=None))
    # Launch was at t=0, but the first successful observation arrives at t=60.
    clock["elapsed"] = 60.0
    start = node.SyncStart(timestamp=_START, counter=1000.0) if clean_start else None

    duration, _, chunk, eras, epochs = node.wait_for_node_to_sync(
        env="mainnet", base_dir=tmp_path, genesis_start=start
    )

    assert duration == (70 if clean_start else 10)
    assert chunk == "00000"
    byron = eras["byron"]
    assert byron["start_epoch"] == (0 if clean_start else 4)
    assert byron["start_time"] == ("epoch-0" if clean_start else "epoch-4")
    assert byron["start_sync_time"] == (
        "2026-10-28T12:00:00Z" if clean_start else "2026-10-28T12:01:00Z"
    )
    assert byron["slots_in_era"] == (4492800 if clean_start else 4406400)
    assert byron["sync_duration_secs"] == (65 if clean_start else 5)
    assert byron["sync_speed_sps"] == int(byron["slots_in_era"] / byron["sync_duration_secs"])
    # Later eras still start at the observed transition, not the process launch.
    assert eras["shelley"]["start_epoch"] == 208
    assert eras["shelley"]["start_sync_time"] == "2026-10-28T12:01:05Z"
    assert eras["shelley"]["sync_duration_secs"] == 5
    # Do not invent epoch timings for the four epochs missed before readiness.
    assert set(epochs) == {4, 208, 209}
    assert epochs[4]["start_sync_time"] == "2026-10-28T12:01:00Z"
    assert epochs[4]["sync_duration_secs"] == 5


@pytest.mark.parametrize("clean_start", [True, False])
@pytest.mark.parametrize("full_sync", [True, False])
def test_orchestration_passes_clean_launch_boundary(
    tmp_path: pl.Path,
    monkeypatch: pytest.MonkeyPatch,
    clock: dict[str, float],
    clean_start: bool,
    full_sync: bool,
) -> None:
    """Only clean full-sync runs backdate measurements to the launch boundary."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "bin").mkdir()
    (tmp_path / "bin" / "cardano-cli").touch()
    for name in [
        "add_to_path",
        "set_node_socket_path_env_var",
        "get_node_files",
        "rm_node_config_files",
        "get_node_config_files",
        "configure_node",
    ]:
        monkeypatch.setattr(node, name, Mock())
    monkeypatch.setattr(sync_entries.helpers, "export_env_var", Mock())
    monkeypatch.setattr(node, "get_node_version", Mock(return_value=("version", "rev")))

    def start_node(**_kwargs: object) -> None:
        clock["elapsed"] += 3

    def wait_node_start(**_kwargs: object) -> int:
        clock["elapsed"] += 60
        return 60

    monkeypatch.setattr(node, "start_node", start_node)
    monkeypatch.setattr(node, "wait_node_start", wait_node_start)
    monkeypatch.setattr(sync_entries.db_sync, "start_monitor", Mock())
    monkeypatch.setattr(sync_entries.helpers, "print_last_n_lines", Mock())
    wait = Mock(return_value=(70, 201000000, "00000", {}, {}))
    monkeypatch.setattr(node, "wait_for_node_to_sync", wait)

    def wait_for_shelley_era(**_kwargs: object) -> None:
        clock["elapsed"] += 7

    monkeypatch.setattr(node, "wait_for_shelley_era", wait_for_shelley_era)
    monkeypatch.setattr(
        node, "get_current_tip", Mock(return_value=_tip("conway", 664, 201000000, 100))
    )

    result = sync_entries.run_node_sync(
        env="mainnet",
        node_revision="11.2.0",
        node_logfile_path=tmp_path / "node_sync.log",
        base_dir=tmp_path,
        conf_dir=tmp_path,
        start_era="conway",
        clean_start=clean_start,
        full_sync=full_sync,
    )

    if full_sync:
        boundary = wait.call_args.kwargs["genesis_start"]
        assert boundary == (
            node.SyncStart(timestamp=_START, counter=1000.0) if clean_start else None
        )
        assert clock["elapsed"] == 63
        assert result.sync_time_sec == 70
    else:
        wait.assert_not_called()
        assert clock["elapsed"] == 70
        assert result.sync_time_sec == 7
