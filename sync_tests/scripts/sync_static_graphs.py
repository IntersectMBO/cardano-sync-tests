#!/usr/bin/env python3
"""CLI script for generating static graphs from sync test result JSON files."""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import pathlib as pl
import sys
import typing as tp

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from matplotlib import ticker

sns.set_theme(style="whitegrid")
LOGGER = logging.getLogger(__name__)


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate sync tests result graphs.")
    parser.add_argument(
        "-i",
        "--json-files",
        required=True,
        type=str,
        nargs="+",
        help="List of JSON file paths to process.",
    )
    parser.add_argument(
        "-o", "--output-dir", required=False, help="Output directory, assume cwd if not provided"
    )
    parser.add_argument("--dpi", type=int, default=150, help="DPI for output images (default: 150)")
    parser.add_argument("--format", type=str, default="png", help="Image format (png, pdf, svg...)")
    parser.add_argument(
        "--mode",
        type=str,
        default="auto",
        choices=["node", "dbsync", "auto"],
        help=(
            "Graph generation mode: node (node-sync graphs), "
            "dbsync (db-sync graphs), auto (detect from JSON)"
        ),
    )
    parser.add_argument(
        "--throughput-mode",
        type=str,
        default="both",
        choices=["raw", "rolling", "both"],
        help=(
            "DB-sync throughput rendering: raw (instantaneous), rolling (smoothed), both (default)"
        ),
    )
    parser.add_argument(
        "--throughput-window",
        type=int,
        default=5,
        help="Rolling average window size in minutes for throughput graph (default: 5)",
    )
    return parser.parse_args()


def detect_json_mode(json_data: dict | list) -> str:
    """Detect whether JSON is node-sync or db-sync format.

    Args:
        json_data: Parsed JSON data from results file (dict or list).

    Returns:
        "node" for node-sync format, "dbsync" for db-sync format.
    """
    # Performance stats JSON (list of metrics) is db-sync format
    if isinstance(json_data, list):
        return "dbsync"

    # DB-sync files have system_metrics (list), epoch_timings, block_insertion_rates
    if isinstance(json_data.get("system_metrics"), list) and "epoch_timings" in json_data:
        return "dbsync"
    # Node-sync files have log_values (dict), sync_duration_per_epoch
    if "log_values" in json_data and isinstance(json_data.get("log_values"), dict):
        return "node"
    # Fallback: check for db-sync specific keys
    if "block_insertion_rates" in json_data or "total_sync_time_in_sec" in json_data:
        return "dbsync"
    # Default to node for backward compatibility
    return "node"


def normalize_dbsync_data(json_data: dict | list) -> dict[str, tp.Any]:
    """Normalize DB-sync JSON to consistent internal format.

    Handles schema variations:
    - db_sync_<env>_results.json: dict with system_metrics, epoch_timings, etc.
    - db_sync_<env>_performance_stats.json: list of system_metrics directly
    - system_metrics as list or dict
    - epoch_timings with various key types

    Args:
        json_data: Raw DB-sync results JSON (dict or list).

    Returns:
        Normalized dict with guaranteed keys: system_metrics (list),
        epoch_timings (dict), block_insertion_rates (list), epoch_details (dict).
    """
    normalized: dict[str, tp.Any] = {}

    # Handle performance_stats.json (list format)
    if isinstance(json_data, list):
        normalized["system_metrics"] = json_data
        normalized["epoch_timings"] = {}
        normalized["block_insertion_rates"] = []
        normalized["epoch_details"] = {}
        return normalized

    # Handle results.json (dict format)
    # Normalize system_metrics
    system_metrics = json_data.get("system_metrics", [])
    if isinstance(system_metrics, dict):
        # Convert dict to list of entries
        system_metrics = list(system_metrics.values()) if system_metrics else []
    normalized["system_metrics"] = system_metrics

    # Normalize epoch_timings (dict with string or int keys)
    epoch_timings_raw = json_data.get("epoch_timings", {})
    epoch_timings = {}
    if isinstance(epoch_timings_raw, dict):
        for key, value in epoch_timings_raw.items():
            try:
                epoch_no = int(key)
                if isinstance(value, dict):
                    # Keep epochs that have valid duration OR valid blocks_count
                    # (duration graph needs duration, blocks graph needs blocks_count)
                    duration = value.get("duration_sec")
                    blocks = value.get("blocks_count", 0)
                    has_valid_duration = duration is not None and duration > 0
                    has_valid_blocks = isinstance(blocks, int | float) and blocks >= 0

                    # Keep if either metric is valid
                    if has_valid_duration or has_valid_blocks:
                        epoch_timings[epoch_no] = value
            except (ValueError, TypeError):
                continue
    normalized["epoch_timings"] = epoch_timings

    # Normalize block_insertion_rates
    block_rates = json_data.get("block_insertion_rates", [])
    if not isinstance(block_rates, list):
        block_rates = []
    # Filter out invalid entries (non-increasing blocks, missing fields)
    valid_rates = []
    last_block = -1
    for entry in block_rates:
        if isinstance(entry, dict):
            block_no = entry.get("block")
            if block_no is not None and block_no > last_block:
                valid_rates.append(entry)
                last_block = block_no
    normalized["block_insertion_rates"] = valid_rates

    # Normalize epoch_details
    epoch_details = json_data.get("epoch_details", {})
    if not isinstance(epoch_details, dict):
        epoch_details = {}
    normalized["epoch_details"] = epoch_details

    return normalized


def generate_static_graphs(
    file_list: list[str],
    output_dir: str,
    dpi: int,
    fmt: str,
    mode: str = "auto",
    throughput_mode: str = "both",
    throughput_window: int = 5,
) -> None:
    LOGGER.info("Starting the sync report generation.")

    output_path = pl.Path(output_dir or ".")
    output_path.mkdir(parents=True, exist_ok=True)

    # First pass: detect mode if auto
    detected_modes = []
    for file_path_str in file_list:
        file_path = pl.Path(file_path_str)
        if not file_path.exists():
            continue
        try:
            with file_path.open(encoding="utf-8") as file:
                json_data = json.load(file)
                detected_modes.append(detect_json_mode(json_data))
        except Exception:
            pass

    # Determine final mode
    if mode == "auto":
        if not detected_modes:
            LOGGER.error("No valid JSON files found for mode detection")
            return
        # Use majority vote with deterministic tie-breaking (prefer dbsync)
        node_count = detected_modes.count("node")
        dbsync_count = detected_modes.count("dbsync")
        mode = "dbsync" if dbsync_count >= node_count else "node"
        LOGGER.info("Auto-detected mode: %s (node=%d, dbsync=%d)", mode, node_count, dbsync_count)
    else:
        LOGGER.info("Using explicit mode: %s", mode)

    # Route to appropriate graph generator
    if mode == "node":
        _generate_node_graphs(file_list, output_path, dpi, fmt)
    elif mode == "dbsync":
        _generate_dbsync_graphs(
            file_list, output_path, dpi, fmt, throughput_mode, throughput_window
        )
    else:
        LOGGER.error("Unknown mode: %s", mode)


def _generate_node_graphs(
    file_list: list[str],
    output_path: pl.Path,
    dpi: int,
    fmt: str,
) -> None:
    """Generate node-sync graphs from node results JSON files."""
    LOGGER.info("Generating node-sync graphs.")
    eras = ["byron", "shelley", "allegra", "mary", "alonzo", "babbage", "conway"]
    log_data = {}
    sync_duration_data = {}
    sync_time_data = {}

    for file_path_str in file_list:
        file_path = pl.Path(file_path_str)
        if not file_path.exists():
            LOGGER.error("File not found: %s", file_path)
            continue

        LOGGER.info("Processing file: %s", file_path)

        try:
            with file_path.open(encoding="utf-8") as file:
                sync_results = json.load(file)
                dataset_name = (
                    sync_results.get("tag_no1")
                    or file_path.name.replace("cardano-node-", "")
                    .replace("sync_results-", "")
                    .replace(".json", "")
                    .strip()
                    or file_path.name.strip()
                )

                log_values_dict = sync_results.get("log_values", {})
                if not log_values_dict:
                    LOGGER.warning("No log_values found in %s", file_path)
                    continue

                log_data[dataset_name] = dict(
                    sorted(log_values_dict.items(), key=lambda item: str(item[0]))
                )

                sync_duration_dict = sync_results.get("sync_duration_per_epoch", {})
                sync_duration_data[dataset_name] = sync_duration_dict

                sync_time_per_era = {
                    era: sync_results.get(f"{era}_sync_duration_secs", 0) for era in eras
                }
                sync_time_data[dataset_name] = sync_time_per_era

        except Exception:
            LOGGER.exception("Unexpected error with file %s", file_path)

    if not log_data:
        LOGGER.error("No valid node-sync data found in provided files")
        return

    LOGGER.info("Generating node-sync results graphs.")
    generate_resource_consumption_graphs(log_data, output_path, dpi=dpi, fmt=fmt)
    generate_duration_per_epoch_graphs(sync_duration_data, output_path, dpi=dpi, fmt=fmt)
    generate_sync_time_per_era_graphs(sync_time_data, eras, output_path, dpi=dpi, fmt=fmt)


def _generate_dbsync_graphs(
    file_list: list[str],
    output_path: pl.Path,
    dpi: int,
    fmt: str,
    throughput_mode: str = "both",
    throughput_window: int = 5,
) -> None:
    """Generate db-sync graphs from db-sync results JSON files.

    Args:
        file_list: List of JSON file paths to process.
        output_path: Output directory for graphs.
        dpi: Image resolution.
        fmt: Image format.
        throughput_mode: Throughput rendering mode (raw/rolling/both).
        throughput_window: Rolling average window size in minutes.
    """
    LOGGER.info("Generating db-sync graphs.")
    datasets: dict[str, dict[str, tp.Any]] = {}

    for file_path_str in file_list:
        file_path = pl.Path(file_path_str)
        if not file_path.exists():
            LOGGER.error("File not found: %s", file_path)
            continue

        LOGGER.info("Processing file: %s", file_path)

        try:
            with file_path.open(encoding="utf-8") as file:
                json_data = json.load(file)
                dataset_name = file_path.name

                # Normalize data
                normalized = normalize_dbsync_data(json_data)

                # Quality assertions
                if not normalized["system_metrics"]:
                    LOGGER.warning("Empty system_metrics in %s, skipping", file_path)
                    continue
                if len(normalized["system_metrics"]) < 10:
                    LOGGER.warning(
                        "Sparse system_metrics (%d samples) in %s",
                        len(normalized["system_metrics"]),
                        file_path,
                    )

                datasets[dataset_name] = normalized

        except Exception:
            LOGGER.exception("Unexpected error with file %s", file_path)

    if not datasets:
        LOGGER.error("No valid db-sync data found in provided files")
        return

    LOGGER.info("Generating db-sync results graphs.")
    generate_dbsync_cpu_graph(datasets, output_path, dpi=dpi, fmt=fmt)
    generate_dbsync_rss_graph(datasets, output_path, dpi=dpi, fmt=fmt)
    generate_dbsync_combined_resources_graph(datasets, output_path, dpi=dpi, fmt=fmt)
    generate_dbsync_epoch_duration_graph(datasets, output_path, dpi=dpi, fmt=fmt)
    generate_dbsync_blocks_per_epoch_graph(datasets, output_path, dpi=dpi, fmt=fmt)
    generate_dbsync_block_throughput_graph(
        datasets,
        output_path,
        dpi=dpi,
        fmt=fmt,
        mode=throughput_mode,
        window_minutes=throughput_window,
    )
    generate_dbsync_era_breakdown_graph(datasets, output_path, dpi=dpi, fmt=fmt)


def generate_resource_consumption_graphs(
    datasets: dict[str, dict],
    output_dir: pl.Path,
    dpi: int,
    fmt: str,
) -> None:
    palette = sns.color_palette("tab10")

    def _plot(
        data_type: str, unit_conversion: float, title: str, ylabel: str, basename: str
    ) -> None:
        plt.figure(figsize=(8, 6))
        for idx, (dataset_name, data) in enumerate(datasets.items()):
            x = []
            y = []
            for value in data.values():
                if value.get("tip") and value.get(data_type):
                    raw = float(value[data_type])
                    if raw > 0.0:
                        x.append(int(value["tip"]))
                        y.append(raw / unit_conversion)
            plt.plot(x, y, label=dataset_name, color=palette[idx % len(palette)])

        plt.title(title)
        plt.xlabel("Slot Number")
        plt.ylabel(ylabel)
        plt.legend()
        plt.tight_layout()
        plt.savefig(output_dir / f"{basename}.{fmt}", dpi=dpi, format=fmt)
        plt.close()

    _plot("rss_ram", 1024**3, "RSS Consumption", "RSS consumed [GB]", "nodesync_rss_consumption")
    _plot("heap_ram", 1024**3, "RAM Consumption", "RAM consumed [GB]", "nodesync_ram_consumption")
    _plot("cpu", 1, "CPU Load", "CPU consumed [%]", "nodesync_cpu_consumption")
    LOGGER.info("Successfully generated node-sync resource consumption graphs.")


def generate_duration_per_epoch_graphs(
    datasets: dict[str, dict],
    output_dir: pl.Path,
    dpi: int,
    fmt: str,
) -> None:
    plt.figure(figsize=(8, 6))
    palette = sns.color_palette("tab10")

    for idx, (dataset_name, data) in enumerate(datasets.items()):
        x = list(map(int, data.keys()))
        y = list(map(float, data.values()))
        plt.plot(x, y, label=dataset_name, color=palette[idx % len(palette)])

    plt.title("Sync Duration per Epoch")
    plt.xlabel("Epoch Number")
    plt.ylabel("Sync duration [seconds]")
    plt.legend()

    ax = plt.gca()
    ax.xaxis.set_major_locator(ticker.MultipleLocator(50))  # Set x-ticks every 50

    plt.tight_layout()
    plt.savefig(output_dir / f"nodesync_duration_per_epoch.{fmt}", dpi=dpi, format=fmt)
    plt.close()
    LOGGER.info("Successfully generated node-sync duration per epoch graph.")


def generate_sync_time_per_era_graphs(
    datasets: dict[str, dict],
    eras: list[str],
    output_dir: pl.Path,
    dpi: int,
    fmt: str,
) -> None:
    dataset_names = list(datasets.keys())
    values_per_era = [[datasets[name][era] for name in dataset_names] for era in eras]

    ind = np.arange(len(dataset_names))
    width = 0.6
    bottoms = np.zeros(len(dataset_names))
    palette = sns.color_palette("pastel", n_colors=len(eras))

    plt.figure(figsize=(8, 6))
    for idx, era in enumerate(eras):
        values = values_per_era[idx]
        plt.bar(ind, values, width, bottom=bottoms, label=era, color=palette[idx])
        bottoms += values

    for i, total in enumerate(bottoms):
        plt.text(ind[i], total + 5, f"{total / 1000:.3f}K", ha="center", va="bottom", fontsize=9)

    plt.xticks(ind, dataset_names, rotation=45, ha="right")
    plt.title("Sync Duration per Era")
    plt.ylabel("Sync duration [seconds]")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_dir / f"nodesync_time_per_era.{fmt}", dpi=dpi, format=fmt)
    plt.close()
    LOGGER.info("Successfully generated node-sync time per era graph.")


def generate_dbsync_cpu_graph(
    datasets: dict[str, dict[str, tp.Any]],
    output_dir: pl.Path,
    dpi: int,
    fmt: str,
) -> None:
    """Generate CPU usage over time graph for db-sync."""
    plt.figure(figsize=(8, 6))
    palette = sns.color_palette("tab10")

    for idx, (dataset_name, data) in enumerate(datasets.items()):
        system_metrics = data.get("system_metrics", [])
        x = []
        y = []
        for entry in system_metrics:
            if isinstance(entry, dict):
                time_val = entry.get("time")
                cpu_val = entry.get("cpu_percent_usage")
                if time_val is not None and cpu_val is not None and cpu_val >= 0:
                    x.append(float(time_val) / 3600)  # Convert seconds to hours
                    y.append(float(cpu_val))

        if x and y:
            plt.plot(x, y, label=dataset_name, color=palette[idx % len(palette)])

    plt.title("DB-Sync CPU Usage Over Time")
    plt.xlabel("Time [hours]")
    plt.ylabel("CPU Usage [%]")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_dir / f"dbsync_cpu_over_time.{fmt}", dpi=dpi, format=fmt)
    plt.close()
    LOGGER.info("Generated db-sync CPU graph.")


def generate_dbsync_rss_graph(
    datasets: dict[str, dict[str, tp.Any]],
    output_dir: pl.Path,
    dpi: int,
    fmt: str,
) -> None:
    """Generate RSS memory usage over time graph for db-sync."""
    plt.figure(figsize=(8, 6))
    palette = sns.color_palette("tab10")

    for idx, (dataset_name, data) in enumerate(datasets.items()):
        system_metrics = data.get("system_metrics", [])
        x = []
        y = []
        for entry in system_metrics:
            if isinstance(entry, dict):
                time_val = entry.get("time")
                rss_val = entry.get("rss_mem_usage")
                if time_val is not None and rss_val is not None and rss_val > 0:
                    x.append(float(time_val) / 3600)  # Convert seconds to hours
                    y.append(float(rss_val) / (1024**3))  # Convert bytes to GB

        if x and y:
            plt.plot(x, y, label=dataset_name, color=palette[idx % len(palette)])

    plt.title("DB-Sync RSS Memory Usage Over Time")
    plt.xlabel("Time [hours]")
    plt.ylabel("RSS Memory [GB]")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_dir / f"dbsync_rss_over_time.{fmt}", dpi=dpi, format=fmt)
    plt.close()
    LOGGER.info("Generated db-sync RSS graph.")


def generate_dbsync_combined_resources_graph(
    datasets: dict[str, dict[str, tp.Any]],
    output_dir: pl.Path,
    dpi: int,
    fmt: str,
) -> None:
    """Generate combined CPU + RSS memory graph with dual Y-axes for db-sync."""
    fig, ax1 = plt.subplots(figsize=(8, 6))
    palette = sns.color_palette("tab10")

    # CPU on left Y-axis
    ax1.set_xlabel("Time [hours]")
    ax1.set_ylabel("CPU Usage [%]", color="tab:blue")
    ax1.tick_params(axis="y", labelcolor="tab:blue")

    # RSS on right Y-axis
    ax2 = ax1.twinx()
    ax2.set_ylabel("RSS Memory [GB]", color="tab:red")
    ax2.tick_params(axis="y", labelcolor="tab:red")

    for idx, (dataset_name, data) in enumerate(datasets.items()):
        system_metrics = data.get("system_metrics", [])
        cpu_times = []
        cpu_values = []
        rss_times = []
        rss_values = []

        for entry in system_metrics:
            if not isinstance(entry, dict):
                continue

            time_val = entry.get("time")
            cpu_val = entry.get("cpu_percent_usage")
            rss_val = entry.get("rss_mem_usage")

            if time_val is not None:
                time_hours = float(time_val) / 3600

                if cpu_val is not None and cpu_val >= 0:
                    cpu_times.append(time_hours)
                    cpu_values.append(float(cpu_val))

                if rss_val is not None and rss_val > 0:
                    rss_times.append(time_hours)
                    rss_values.append(float(rss_val) / (1024**3))  # Convert bytes to GB

        color = palette[idx % len(palette)]
        if cpu_times and cpu_values:
            ax1.plot(
                cpu_times,
                cpu_values,
                label=f"{dataset_name} (CPU)",
                color=color,
                linestyle="-",
                alpha=0.8,
            )
        if rss_times and rss_values:
            ax2.plot(
                rss_times,
                rss_values,
                label=f"{dataset_name} (RSS)",
                color=color,
                linestyle="--",
                alpha=0.8,
            )

    # Combine legends from both axes
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    plt.title("DB-Sync CPU and Memory Usage Over Time")
    fig.tight_layout()
    plt.savefig(output_dir / f"dbsync_combined_resources.{fmt}", dpi=dpi, format=fmt)
    plt.close()
    LOGGER.info("Generated db-sync combined resources graph.")


def generate_dbsync_epoch_duration_graph(
    datasets: dict[str, dict[str, tp.Any]],
    output_dir: pl.Path,
    dpi: int,
    fmt: str,
) -> None:
    """Generate sync duration per epoch graph for db-sync."""
    plt.figure(figsize=(8, 6))
    palette = sns.color_palette("tab10")

    has_data = False
    for idx, (dataset_name, data) in enumerate(datasets.items()):
        epoch_timings = data.get("epoch_timings", {})
        if not epoch_timings:
            continue

        x = []
        y = []
        for epoch_no, timing in sorted(epoch_timings.items()):
            if not isinstance(timing, dict):
                continue
            duration = timing.get("duration_sec")
            if duration is not None and duration > 0:
                x.append(int(epoch_no))
                y.append(float(duration))

        if x and y:
            plt.plot(x, y, label=dataset_name, color=palette[idx % len(palette)])
            has_data = True

    plt.title("DB-Sync Duration per Epoch")
    plt.xlabel("Epoch Number")
    plt.ylabel("Duration [seconds]")

    if has_data:
        plt.legend()
    else:
        plt.text(
            0.5,
            0.5,
            "No epoch duration data available",
            ha="center",
            va="center",
            transform=plt.gca().transAxes,
            fontsize=12,
        )
        LOGGER.warning("No valid epoch duration data found for plotting")

    ax = plt.gca()
    ax.xaxis.set_major_locator(ticker.MultipleLocator(50))

    plt.tight_layout()
    plt.savefig(output_dir / f"dbsync_epoch_duration.{fmt}", dpi=dpi, format=fmt)
    plt.close()
    LOGGER.info("Generated db-sync epoch duration graph.")


def generate_dbsync_blocks_per_epoch_graph(
    datasets: dict[str, dict[str, tp.Any]],
    output_dir: pl.Path,
    dpi: int,
    fmt: str,
) -> None:
    """Generate blocks per epoch graph for db-sync."""
    plt.figure(figsize=(8, 6))
    palette = sns.color_palette("tab10")

    has_data = False
    all_zeros = True
    for idx, (dataset_name, data) in enumerate(datasets.items()):
        epoch_timings = data.get("epoch_timings", {})
        if not epoch_timings:
            continue

        x = []
        y = []
        for epoch_no, timing in sorted(epoch_timings.items()):
            if not isinstance(timing, dict):
                continue
            blocks_count = timing.get("blocks_count", 0)
            # Accept any non-negative block count (including 0 for empty epochs)
            if isinstance(blocks_count, int | float) and blocks_count >= 0:
                x.append(int(epoch_no))
                y.append(int(blocks_count))
                if blocks_count > 0:
                    all_zeros = False

        if x and y:
            plt.plot(x, y, label=dataset_name, color=palette[idx % len(palette)], linewidth=2)
            has_data = True

    plt.title("DB-Sync Blocks per Epoch")
    plt.xlabel("Epoch Number")
    plt.ylabel("Block Count")

    if has_data:
        plt.legend()
        # If all values are zero, adjust Y-axis to make the line visible
        if all_zeros:
            plt.ylim(-0.5, 10)  # Show range from -0.5 to 10 so zero line is visible
            plt.text(
                0.5,
                0.9,
                "Warning: all epochs have 0 blocks (data quality issue)",
                ha="center",
                va="top",
                transform=plt.gca().transAxes,
                fontsize=10,
                style="italic",
                bbox={"boxstyle": "round", "facecolor": "yellow", "alpha": 0.3},
            )
            LOGGER.warning("All epochs have blocks_count = 0 (poor data quality)")
    else:
        plt.text(
            0.5,
            0.5,
            "No epoch blocks data available",
            ha="center",
            va="center",
            transform=plt.gca().transAxes,
            fontsize=12,
        )
        LOGGER.warning("No valid epoch blocks data found for plotting")

    ax = plt.gca()
    ax.xaxis.set_major_locator(ticker.MultipleLocator(50))

    plt.tight_layout()
    plt.savefig(output_dir / f"dbsync_blocks_per_epoch.{fmt}", dpi=dpi, format=fmt)
    plt.close()
    LOGGER.info("Generated db-sync blocks per epoch graph.")


def generate_dbsync_block_throughput_graph(
    datasets: dict[str, dict[str, tp.Any]],
    output_dir: pl.Path,
    dpi: int,
    fmt: str,
    mode: str = "both",
    window_minutes: int = 5,
) -> None:
    """Generate block throughput with configurable visualization mode.

    Args:
        datasets: Normalized DB-sync datasets.
        output_dir: Output directory for graph.
        dpi: Image resolution.
        fmt: Image format.
        mode: Visualization mode - "raw" (instantaneous), "rolling" (smoothed), "both" (default).
        window_minutes: Rolling average window size in minutes (default 5).
    """
    plt.figure(figsize=(8, 6))
    palette = sns.color_palette("tab10")

    for idx, (dataset_name, data) in enumerate(datasets.items()):
        block_rates = data.get("block_insertion_rates", [])
        if len(block_rates) < 2:
            continue

        # Calculate instantaneous throughput (blocks per minute)
        times = []
        throughputs = []
        first_timestamp: datetime.datetime | None = None

        for i in range(1, len(block_rates)):
            prev_entry = block_rates[i - 1]
            curr_entry = block_rates[i]

            if not isinstance(prev_entry, dict) or not isinstance(curr_entry, dict):
                continue

            prev_ts = prev_entry.get("timestamp")
            curr_ts = curr_entry.get("timestamp")
            prev_block = prev_entry.get("block")
            curr_block = curr_entry.get("block")

            if not all([prev_ts, curr_ts, prev_block is not None, curr_block is not None]):
                continue

            # Type guards for mypy
            if not isinstance(prev_ts, str) or not isinstance(curr_ts, str):
                continue
            if not isinstance(prev_block, int) or not isinstance(curr_block, int):
                continue

            try:
                prev_dt = datetime.datetime.fromisoformat(prev_ts)
                curr_dt = datetime.datetime.fromisoformat(curr_ts)

                # Set first timestamp for elapsed time calculation
                if first_timestamp is None:
                    first_timestamp = prev_dt

                delta_sec = (curr_dt - prev_dt).total_seconds()

                if delta_sec > 0:
                    delta_blocks = curr_block - prev_block
                    throughput = (delta_blocks / delta_sec) * 60  # Blocks per minute
                    elapsed_hours = (curr_dt - first_timestamp).total_seconds() / 3600
                    times.append(elapsed_hours)
                    throughputs.append(throughput)
            except (ValueError, TypeError):
                continue

        if len(times) < 10:
            LOGGER.warning("Insufficient throughput data for %s", dataset_name)
            continue

        color = palette[idx % len(palette)]

        # Plot raw (instantaneous) throughput
        if mode in ("raw", "both"):
            plt.plot(
                times,
                throughputs,
                label=f"{dataset_name} (raw)" if mode == "both" else dataset_name,
                color=color,
                alpha=0.3 if mode == "both" else 0.8,
                linewidth=0.5 if mode == "both" else 1.0,
            )

        # Plot rolling average throughput
        if mode in ("rolling", "both"):
            # Estimate samples per second (assume roughly uniform sampling)
            total_time_seconds = times[-1] * 3600 if times else 0
            samples_per_second = (
                len(throughputs) / total_time_seconds if total_time_seconds > 0 else 1
            )
            window_size = int(window_minutes * 60 * samples_per_second)
            # Clamp between 5 and 10% of data.
            window_size = max(5, min(window_size, len(throughputs) // 10))

            smoothed = []
            for i in range(len(throughputs)):
                start_idx = max(0, i - window_size // 2)
                end_idx = min(len(throughputs), i + window_size // 2 + 1)
                smoothed.append(np.mean(throughputs[start_idx:end_idx]))

            plt.plot(
                times,
                smoothed,
                label=f"{dataset_name} (rolling)" if mode == "both" else dataset_name,
                color=color,
                alpha=0.9,
                linewidth=2.0 if mode == "both" else 1.5,
            )

    # Dynamic title based on mode
    if mode == "raw":
        title = "DB-Sync Block Throughput (instantaneous)"
    elif mode == "rolling":
        title = f"DB-Sync Block Throughput ({window_minutes}-min rolling average)"
    else:  # both
        title = f"DB-Sync Block Throughput (raw + {window_minutes}-min rolling)"

    plt.title(title)
    plt.xlabel("Time [hours]")
    plt.ylabel("Block Insertion Rate [blocks/minute]")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_dir / f"dbsync_block_throughput.{fmt}", dpi=dpi, format=fmt)
    plt.close()
    LOGGER.info(
        "Generated db-sync block throughput graph (mode=%s, window=%d min).",
        mode,
        window_minutes,
    )


def generate_dbsync_era_breakdown_graph(
    datasets: dict[str, dict[str, tp.Any]],
    output_dir: pl.Path,
    dpi: int,
    fmt: str,
) -> None:
    """Generate era breakdown graph for db-sync (sync time per era)."""
    # First pass: collect all unique eras from all datasets
    all_eras: set[str] = set()

    for data in datasets.values():
        system_metrics = data.get("system_metrics", [])
        for entry in system_metrics:
            if isinstance(entry, dict):
                era_name = entry.get("era_name")
                if era_name:
                    # Normalize era name (extract base name before parentheses)
                    base_era = era_name.split("(")[0].strip()
                    all_eras.add(base_era)

    # Sort eras in traditional order, with unknown/new eras at the end
    traditional_order = ["Byron", "Shelley", "Allegra", "Mary", "Alonzo", "Babbage", "Conway"]
    era_names = [era for era in traditional_order if era in all_eras]
    # Add any additional eras not in traditional list (e.g., Chang 2)
    era_names.extend(sorted(era for era in all_eras if era not in traditional_order))

    if not era_names:
        LOGGER.warning("No era data found for era breakdown graph")
        era_names = traditional_order  # Fallback to traditional list

    dataset_names = list(datasets.keys())
    era_durations: dict[str, list[float]] = {era: [0.0] * len(dataset_names) for era in era_names}

    for ds_idx, (_dataset_name, data) in enumerate(datasets.items()):
        system_metrics = data.get("system_metrics", [])

        # Group metrics by era and calculate time spent in each era
        era_time: dict[str, float] = {}
        era_start_time: dict[str, float] = {}
        last_time = 0.0

        for entry in system_metrics:
            if not isinstance(entry, dict):
                continue

            time_val = entry.get("time")
            era_name = entry.get("era_name", "Unknown")

            if time_val is None:
                continue

            time_val = float(time_val)
            last_time = time_val

            # Track when this era started
            if era_name not in era_start_time:
                era_start_time[era_name] = time_val

        # Calculate duration for each era
        era_list = list(era_start_time.items())
        for i, (era_name, start_time) in enumerate(era_list):
            if i < len(era_list) - 1:
                # Duration is from this era's start to next era's start
                next_start = era_list[i + 1][1]
                era_time[era_name] = next_start - start_time
            else:
                # Last era: duration from start to end of sync
                era_time[era_name] = last_time - start_time

        # Normalize era names (handle "Alonzo (Intra-Era)" → "Alonzo", etc.)
        normalized_era_time: dict[str, float] = {}
        for era_raw, duration in era_time.items():
            # Extract base era name (before any parentheses)
            base_era = era_raw.split("(")[0].strip()
            normalized_era_time[base_era] = normalized_era_time.get(base_era, 0.0) + duration

        # Populate era_durations matrix
        for era in era_names:
            era_durations[era][ds_idx] = normalized_era_time.get(era, 0.0)

    # Create stacked bar chart
    ind = np.arange(len(dataset_names))
    width = 0.6
    bottoms = np.zeros(len(dataset_names))
    palette = sns.color_palette("pastel", n_colors=len(era_names))

    plt.figure(figsize=(8, 6))
    for idx, era in enumerate(era_names):
        values = era_durations[era]
        if sum(values) > 0:  # Only plot eras with non-zero data
            plt.bar(ind, values, width, bottom=bottoms, label=era, color=palette[idx])
            bottoms += values

    for i, total in enumerate(bottoms):
        if total > 0:
            plt.text(
                ind[i],
                total + total * 0.02,
                f"{total / 3600:.1f}h",
                ha="center",
                va="bottom",
                fontsize=9,
            )

    plt.xticks(ind, dataset_names, rotation=45, ha="right")
    plt.title("DB-Sync Time per Era")
    plt.ylabel("Sync Duration [seconds]")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_dir / f"dbsync_era_breakdown.{fmt}", dpi=dpi, format=fmt)
    plt.close()
    LOGGER.info("Generated db-sync era breakdown graph.")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    args = get_args()
    generate_static_graphs(
        file_list=args.json_files,
        output_dir=args.output_dir or "",
        dpi=args.dpi,
        fmt=args.format,
        mode=args.mode,
        throughput_mode=args.throughput_mode,
        throughput_window=args.throughput_window,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
