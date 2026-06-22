"""Chart generation utilities for DB Sync performance statistics."""

from __future__ import annotations

import json
import logging

import matplotlib.pyplot as plt

from sync_tests.utils.db_sync.config import DbSyncConfig
from sync_tests.utils.path_utils import get_db_sync_dir

LOGGER = logging.getLogger(__name__)


def create_sync_stats_chart(config: DbSyncConfig) -> None:
    """Create a chart showing sync statistics.

    Args:
        config: A DbSyncConfig instance with paths.
    """
    db_sync_dir = get_db_sync_dir()
    fig = plt.figure(figsize=(14, 10))

    ax_epochs = fig.add_axes((0.05, 0.05, 0.9, 0.35))
    ax_epochs.set(xlabel="epochs [number]", ylabel="time [min]")
    ax_epochs.set_title("Epochs Sync Times")

    with open(config.epoch_sync_times_file) as json_db_dump_file:
        epoch_sync_times = json.load(json_db_dump_file)

    epochs = [e["no"] for e in epoch_sync_times]
    epoch_times = [e["seconds"] / 60 for e in epoch_sync_times]
    ax_epochs.bar(epochs, epoch_times)

    ax_perf = fig.add_axes((0.05, 0.5, 0.9, 0.45))
    ax_perf.set(xlabel="time [min]", ylabel="RSS [B]")
    ax_perf.set_title("RSS usage")

    with open(config.perf_stats_file) as json_db_dump_file:
        perf_stats_payload = json.load(json_db_dump_file)

    if isinstance(perf_stats_payload, dict):
        perf_stats = perf_stats_payload.get("system_metrics", [])
    else:
        perf_stats = perf_stats_payload

    times = [e["time"] / 60 for e in perf_stats]
    rss_mem_usage = [e["rss_mem_usage"] for e in perf_stats]

    ax_perf.plot(times, rss_mem_usage)
    chart_path = db_sync_dir / config.chart_name
    fig.savefig(chart_path)
    plt.close(fig)
