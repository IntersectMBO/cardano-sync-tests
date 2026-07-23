"""Tests for sync_tests.scripts.sync_static_graphs."""

from __future__ import annotations

import json
import pathlib as pl

from sync_tests.scripts.sync_static_graphs import detect_json_mode
from sync_tests.scripts.sync_static_graphs import generate_static_graphs
from sync_tests.scripts.sync_static_graphs import normalize_dbsync_data


class TestDetectJsonMode:
    def test_list_is_dbsync(self) -> None:
        assert detect_json_mode([{"time": 0, "rss_mem_usage": 1}]) == "dbsync"

    def test_dict_with_system_metrics_list_and_epoch_timings_is_dbsync(self) -> None:
        data = {"system_metrics": [{"time": 0}], "epoch_timings": {}}
        assert detect_json_mode(data) == "dbsync"

    def test_dict_with_log_values_is_node(self) -> None:
        data = {"log_values": {"2026-01-01T00:00:00": {"tip": 1}}}
        assert detect_json_mode(data) == "node"

    def test_dict_with_block_insertion_rates_fallback_is_dbsync(self) -> None:
        assert detect_json_mode({"block_insertion_rates": []}) == "dbsync"

    def test_dict_with_total_sync_time_fallback_is_dbsync(self) -> None:
        assert detect_json_mode({"total_sync_time_in_sec": 100}) == "dbsync"

    def test_unrecognized_dict_defaults_to_node(self) -> None:
        assert detect_json_mode({"some_other_key": 1}) == "node"


class TestNormalizeDbsyncData:
    def test_list_input_becomes_system_metrics(self) -> None:
        data = [{"time": 0, "rss_mem_usage": 100}]
        result = normalize_dbsync_data(data)
        assert result["system_metrics"] == data
        assert result["epoch_timings"] == {}
        assert result["block_insertion_rates"] == []
        assert result["epoch_details"] == {}

    def test_system_metrics_dict_converted_to_list(self) -> None:
        data = {"system_metrics": {"a": {"time": 0}, "b": {"time": 1}}}
        result = normalize_dbsync_data(data)
        assert result["system_metrics"] == [{"time": 0}, {"time": 1}]

    def test_epoch_timings_keeps_entries_with_valid_duration_or_blocks(self) -> None:
        data = {
            "epoch_timings": {
                "1": {"duration_sec": 120, "blocks_count": 0},
                "2": {"duration_sec": None, "blocks_count": 5},
                "3": {"duration_sec": 0, "blocks_count": 0},
                "not-a-number": {"duration_sec": 120, "blocks_count": 1},
            }
        }
        result = normalize_dbsync_data(data)
        # Epoch 3 has neither a positive duration nor a truthy-enough blocks_count
        # (0 still counts as valid per has_valid_blocks, so it IS kept)
        assert set(result["epoch_timings"].keys()) == {1, 2, 3}
        assert result["epoch_timings"][1]["duration_sec"] == 120

    def test_epoch_timings_drops_non_integer_keys(self) -> None:
        data = {"epoch_timings": {"abc": {"duration_sec": 100, "blocks_count": 1}}}
        result = normalize_dbsync_data(data)
        assert result["epoch_timings"] == {}

    def test_block_insertion_rates_filters_non_increasing_blocks(self) -> None:
        data = {
            "block_insertion_rates": [
                {"block": 100, "time": 0},
                {"block": 101, "time": 1},
                {"block": 101, "time": 2},  # not strictly increasing, dropped
                {"block": 99, "time": 3},  # goes backwards, dropped
                {"block": 200, "time": 4},
            ]
        }
        result = normalize_dbsync_data(data)
        assert [entry["block"] for entry in result["block_insertion_rates"]] == [100, 101, 200]

    def test_epoch_details_defaults_to_empty_dict_when_not_a_dict(self) -> None:
        result = normalize_dbsync_data({"epoch_details": "not-a-dict"})
        assert result["epoch_details"] == {}


def test_generate_static_graphs_dbsync_mode_produces_pngs(tmp_path: pl.Path) -> None:
    results = {
        "system_metrics": [
            {"time": 0, "rss_mem_usage": 1_000_000, "cpu_percent_usage": 10},
            {"time": 60, "rss_mem_usage": 1_100_000, "cpu_percent_usage": 15},
        ],
        "epoch_timings": {
            "1": {"start_time": None, "end_time": None, "duration_sec": 120, "blocks_count": 5},
        },
        "block_insertion_rates": [],
        "epoch_details": {},
    }
    results_file = tmp_path / "db_sync_preview_results.json"
    results_file.write_text(json.dumps(results), encoding="utf-8")
    output_dir = tmp_path / "graphs"

    generate_static_graphs(
        file_list=[str(results_file)],
        output_dir=str(output_dir),
        dpi=50,
        fmt="png",
        mode="dbsync",
    )

    produced = {p.name for p in output_dir.glob("*.png")}
    assert produced, "expected at least one PNG to be generated"
    assert all(p.stat().st_size > 0 for p in output_dir.glob("*.png"))
