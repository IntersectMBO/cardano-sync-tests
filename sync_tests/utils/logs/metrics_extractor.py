"""Extract performance metrics from cardano-node log files."""

from __future__ import annotations

import datetime
import heapq
import itertools
import pathlib as pl
import re
import typing as tp

from sync_tests.utils.logs.filtering import open_filtered_log_fd


def merge_sorted_unique(*iterables: tp.Iterable) -> list:
    """Merge and sort multiple sorted iterables while removing duplicates."""
    return [key for key, _ in itertools.groupby(heapq.merge(*iterables))]


def get_data_from_logs(log_file: pl.Path) -> dict[str, dict]:
    """Extract relevant data from the log file and return a dictionary."""
    chunk_size = 512 * 1024  # 512 KB
    tip_details_dict: dict[datetime.datetime, int] = {}
    heap_ram_details_dict: dict[datetime.datetime, float] = {}
    rss_ram_details_dict: dict[datetime.datetime, float] = {}
    centi_cpu_dict: dict[datetime.datetime, float] = {}
    cpu_ticks_dict: dict[datetime.datetime, float] = {}
    cpu_details_dict: dict[datetime.datetime, float] = {}
    logs_details_dict: dict[str, dict[str, tp.Any]] = {}

    timestamp_pattern = re.compile(r"\d{4}-\d{2}-\d{2} \d{1,2}:\d{1,2}:\d{1,2}")
    heap_pattern = re.compile(r'"Heap",Number ([-+]?\d+\.?\d*(?:[Ee][-+]?\d+)?)')
    rss_pattern = re.compile(r'"RSS",Number ([-+]?\d+\.?\d*(?:[Ee][-+]?\d+)?)')
    centi_cpu_pattern = re.compile(
        r'"CentiCpu",Number ([-+]?\d+\.?\d*(?:[Ee][-+]?\d+)?)',
    )
    resources_human_pattern = re.compile(
        r"Resources:\s+Cpu Ticks\s+(\d+).+?RTS heap\s+(\d+),\s+RSS\s+(\d+)",
        re.IGNORECASE,
    )

    def _process_log_line(line: str) -> None:
        """Extract relevant data from a log line and updates dictionaries."""
        # Extract numeric values for heap, RSS, and CPU if they exist
        if (
            "cardano.node.resources" in line
            and (timestamp_match := timestamp_pattern.search(line))
            and (heap_match := heap_pattern.search(line))
            and (rss_match := rss_pattern.search(line))
            and (centi_cpu_match := centi_cpu_pattern.search(line))
        ):
            timestamp = datetime.datetime.strptime(
                timestamp_match.group(0), "%Y-%m-%d %H:%M:%S"
            ).replace(tzinfo=datetime.timezone.utc)
            heap_ram_details_dict[timestamp] = float(heap_match.group(1))
            rss_ram_details_dict[timestamp] = float(rss_match.group(1))
            centi_cpu_dict[timestamp] = float(centi_cpu_match.group(1))
        elif (
            "Resources:" in line
            and (timestamp_match := timestamp_pattern.search(line))
            and (resources_match := resources_human_pattern.search(line))
        ):
            timestamp = datetime.datetime.strptime(
                timestamp_match.group(0), "%Y-%m-%d %H:%M:%S"
            ).replace(tzinfo=datetime.timezone.utc)
            cpu_ticks_dict[timestamp] = float(resources_match.group(1))
            heap_ram_details_dict[timestamp] = float(resources_match.group(2))
            rss_ram_details_dict[timestamp] = float(resources_match.group(3))

        # Extract slot number
        elif (
            "new tip" in line
            and " at slot " in line
            and (timestamp_match := timestamp_pattern.search(line))
        ):
            timestamp = datetime.datetime.strptime(
                timestamp_match.group(0), "%Y-%m-%d %H:%M:%S"
            ).replace(tzinfo=datetime.timezone.utc)
            tip_details_dict[timestamp] = int(line.split(" at slot ", 1)[1])

    def _process_log_file(infile: tp.IO) -> None:
        # Read the file in chunks to handle large logs efficiently
        incomplete_line = ""
        while chunk := infile.read(chunk_size):
            if incomplete_line:
                chunk = incomplete_line + chunk  # Prepend leftover from previous chunk

            lines = chunk.splitlines(keepends=False)

            # Handle incomplete lines at the end of the chunk
            incomplete_line = lines.pop() if chunk[-1] not in "\n\r" else ""

            # Process each complete log line
            for line in lines:
                _process_log_line(line)

        # Process any remaining incomplete line
        if incomplete_line:
            _process_log_line(incomplete_line)

    filter_pattern = r"cardano\.node\.resources|Resources:|new tip"

    process = open_filtered_log_fd(
        log_file=log_file,
        pattern=filter_pattern,
        tool="rg",
    )
    if process:
        with process:
            if process.stdout:
                _process_log_file(infile=process.stdout)

    if not centi_cpu_dict and not cpu_ticks_dict:
        process = open_filtered_log_fd(
            log_file=log_file,
            pattern=filter_pattern,
            tool="grep",
        )
        if process:
            with process:
                if process.stdout:
                    _process_log_file(infile=process.stdout)
    # If neither 'rg' nor 'grep' is available, read the log file directly without filtering
    if not centi_cpu_dict and not cpu_ticks_dict:
        with open(log_file, encoding="utf-8") as infile:
            _process_log_file(infile=infile)

    cpu_source = centi_cpu_dict or cpu_ticks_dict
    cpu_multiplier = 1.0 if centi_cpu_dict else 100.0
    for prev_timestamp, curr_timestamp in itertools.pairwise(cpu_source):
        prev_value = cpu_source[prev_timestamp]
        curr_value = cpu_source[curr_timestamp]
        elapsed = (curr_timestamp - prev_timestamp).total_seconds()
        if elapsed <= 0:
            continue

        # Compute CPU load percentage over elapsed time (no per-core split; matches CentiCpu scale).
        cpu_load_percent = ((curr_value - prev_value) * cpu_multiplier) / elapsed
        cpu_details_dict[curr_timestamp] = cpu_load_percent

    # Collect all unique timestamps from different dictionaries
    all_timestamps_list = merge_sorted_unique(
        tip_details_dict, cpu_details_dict, rss_ram_details_dict, heap_ram_details_dict
    )

    # Populate logs_details_dict with merged data
    tip = -1
    for timestamp in all_timestamps_list:
        # Use the last known tip if no new tip is available
        tip = tip_details_dict.get(timestamp, tip)
        if tip == -1:
            continue
        cpu = cpu_details_dict.get(timestamp)
        if cpu is None and cpu_details_dict:
            # Carry forward previous CPU sample if current timestamp has only tip/RSS update.
            earlier_cpu_ts = [ts for ts in cpu_details_dict if ts <= timestamp]
            if earlier_cpu_ts:
                latest_ts = max(earlier_cpu_ts)
                cpu = cpu_details_dict.get(latest_ts)
        if cpu is None:
            continue
        logs_details_dict[str(timestamp)] = {
            "tip": tip,
            "heap_ram": heap_ram_details_dict.get(timestamp, 0.0),
            "rss_ram": rss_ram_details_dict.get(timestamp, 0.0),
            "cpu": cpu,
        }

    return logs_details_dict
