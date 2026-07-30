"""Tests for sync_tests.utils.logs.metrics_extractor (node log parsing)."""

from __future__ import annotations

import pathlib as pl

from sync_tests.utils.logs import metrics_extractor


def test_merge_sorted_unique_dedupes_across_already_sorted_inputs() -> None:
    """merge_sorted_unique wraps heapq.merge, which requires each input pre-sorted."""
    result = metrics_extractor.merge_sorted_unique([1, 2, 3], [2, 4], [1])
    assert result == [1, 2, 3, 4]


def test_get_data_from_logs_empty_file(tmp_path: pl.Path) -> None:
    log_file = tmp_path / "node.log"
    log_file.write_text("nothing relevant here\n", encoding="utf-8")

    assert metrics_extractor.get_data_from_logs(log_file) == {}


def test_get_data_from_logs_json_style_resources(tmp_path: pl.Path) -> None:
    """CentiCpu-style (JSON tracer) resource lines, tip carried forward between samples."""
    log_file = tmp_path / "node.log"
    log_file.write_text(
        "2026-01-01 10:00:00 cardano.node.resources trace: "
        '"Heap",Number 1000.0 "RSS",Number 2000.0 "CentiCpu",Number 500.0\n'
        "[2026-01-01 10:00:05.00 UTC] node ChainDB new tip 0xabc at slot 100\n"
        "2026-01-01 10:00:10 cardano.node.resources trace: "
        '"Heap",Number 1100.0 "RSS",Number 2100.0 "CentiCpu",Number 600.0\n'
        "[2026-01-01 10:00:15.00 UTC] node ChainDB new tip 0xdef at slot 200\n",
        encoding="utf-8",
    )

    result = metrics_extractor.get_data_from_logs(log_file)

    # Samples before the first CPU delta (10:00:00) or before any CPU sample exists
    # yet (10:00:05) are dropped; only timestamps with a resolvable CPU value remain.
    assert set(result.keys()) == {
        "2026-01-01 10:00:10+00:00",
        "2026-01-01 10:00:15+00:00",
    }
    at_10 = result["2026-01-01 10:00:10+00:00"]
    assert at_10["tip"] == 100
    assert at_10["heap_ram"] == 1100.0
    assert at_10["rss_ram"] == 2100.0
    # (600 - 500) CentiCpu over 10s, multiplier 1.0 for CentiCpu source.
    assert at_10["cpu"] == 10.0

    at_15 = result["2026-01-01 10:00:15+00:00"]
    assert at_15["tip"] == 200
    # No resource sample exactly at 10:00:15, RSS/heap default to 0.0.
    assert at_15["heap_ram"] == 0.0
    assert at_15["rss_ram"] == 0.0
    # CPU carried forward from the last known sample (10:00:10).
    assert at_15["cpu"] == 10.0


def test_get_data_from_logs_human_readable_resources(tmp_path: pl.Path) -> None:
    """Cpu Ticks-style (human tracer) resource lines use a x100 multiplier."""
    log_file = tmp_path / "node.log"
    log_file.write_text(
        "[2026-01-01 10:00:00.00 UTC] node Resources: Cpu Ticks 1000, "
        "GC centiseconds 5, Mutator centiseconds 300, RTS heap 5000, RSS 6000, extra info\n"
        "[2026-01-01 10:00:05.00 UTC] node ChainDB new tip 0xabc at slot 50\n"
        "[2026-01-01 10:00:10.00 UTC] node Resources: Cpu Ticks 1050, "
        "GC centiseconds 6, Mutator centiseconds 310, RTS heap 5100, RSS 6100, extra info\n",
        encoding="utf-8",
    )

    result = metrics_extractor.get_data_from_logs(log_file)

    assert set(result.keys()) == {"2026-01-01 10:00:10+00:00"}
    entry = result["2026-01-01 10:00:10+00:00"]
    assert entry["tip"] == 50
    assert entry["heap_ram"] == 5100.0
    assert entry["rss_ram"] == 6100.0
    # (1050 - 1000) ticks over 10s, x100 multiplier for the ticks source.
    assert entry["cpu"] == 500.0
