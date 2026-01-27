from __future__ import annotations

import json
import logging
from pathlib import Path

import matplotlib.pyplot as plt

from sync_tests.utils import db_sync

LOGGER = logging.getLogger(__name__)


def _get_repo_root() -> Path:
    """Get the repository root directory.

    This function uses __file__ to reliably find the repo root regardless of
    the current working directory (which may change due to os.chdir() calls).

    Returns:
        Path: The repository root directory.
    """
    # This file is at sync_tests/utils/charts.py
    # Go up 2 levels to get repo root
    return Path(__file__).parent.parent.parent


def _get_db_sync_dir() -> Path:
    """Get the cardano-db-sync directory path.

    The cardano-db-sync repository is cloned to the repo root, not to test_workdir.

    Returns:
        Path: The cardano-db-sync directory path.
    """
    return _get_repo_root() / "cardano-db-sync"


def create_sync_stats_chart(config: db_sync.DbSyncConfig) -> None:
    """Create a chart showing sync statistics.

    Args:
        config: A DbSyncConfig instance with paths.
    """
    db_sync_dir = _get_db_sync_dir()
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
        perf_stats = json.load(json_db_dump_file)

    times = [e["time"] / 60 for e in perf_stats]
    rss_mem_usage = [e["rss_mem_usage"] for e in perf_stats]

    ax_perf.plot(times, rss_mem_usage)
    chart_path = db_sync_dir / config.chart_name
    fig.savefig(chart_path)

