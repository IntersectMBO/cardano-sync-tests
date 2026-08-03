"""Tests for sync_tests.utils.db_sync.metrics_extractor (db-sync log parsing)."""

from __future__ import annotations

import pathlib as pl

import pytest

from sync_tests.utils.db_sync import metrics_extractor


@pytest.mark.parametrize(
    ("time_str", "expected"),
    [
        ("00:00:01.30", 1.3),
        ("01:02:03", 3723.0),
        ("00:00:00.00", 0.0),
        ("00:01:00.50", 60.5),
    ],
)
def test_parse_time_duration_valid(time_str: str, expected: float) -> None:
    assert metrics_extractor._parse_time_duration(time_str) == expected


def test_parse_time_duration_malformed_returns_zero() -> None:
    assert metrics_extractor._parse_time_duration("not-a-duration") == 0.0


def test_get_db_sync_data_from_logs_full_epoch_cycle(tmp_path: pl.Path) -> None:
    log_file = tmp_path / "db_sync.log"
    log_file.write_text(
        "[2026-01-01 10:00:00.00 UTC] Starting epoch 5\n"
        "[2026-01-01 10:00:01.00 UTC] Insert Babbage Block: epoch 5, slot 1000, block 100\n"
        "[2026-01-01 10:00:02.00 UTC] Insert Babbage Block: epoch 5, slot 1001, block 101\n"
        "[2026-01-01 10:00:03.00 UTC] Statistics for Epoch 5\n"
        "[2026-01-01 10:00:03.00 UTC] This epoch took: 00:00:03.00 to process.\n"
        "[2026-01-01 10:00:04.00 UTC] Inserted epoch 5 from updateEpochWhenSyncing\n",
        encoding="utf-8",
    )

    result = metrics_extractor.get_db_sync_data_from_logs(log_file)

    assert len(result["block_insertions"]) == 2
    first, second = result["block_insertions"]
    assert first == {
        "timestamp": "2026-01-01T10:00:01+00:00",
        "epoch": 5,
        "slot": 1000,
        "block": 100,
        "era": "Babbage",
    }
    assert second["slot"] == 1001
    assert second["block"] == 101

    assert set(result["epoch_timings"].keys()) == {5}
    timing = result["epoch_timings"][5]
    assert timing["start_time"] == "2026-01-01T10:00:00+00:00"
    assert timing["end_time"] == "2026-01-01T10:00:03+00:00"
    assert timing["duration_sec"] == 3.0
    assert timing["blocks_count"] == 2

    # epoch_details is finalized from epoch_timings at the end of the function.
    assert result["epoch_details"][5]["duration_sec"] == 3.0
    assert result["epoch_details"][5]["blocks_count"] == 2


def test_get_db_sync_data_from_logs_empty_file(tmp_path: pl.Path) -> None:
    log_file = tmp_path / "db_sync.log"
    log_file.write_text("nothing relevant here\n", encoding="utf-8")

    result = metrics_extractor.get_db_sync_data_from_logs(log_file)

    assert result == {"epoch_timings": {}, "block_insertions": [], "epoch_details": {}}
