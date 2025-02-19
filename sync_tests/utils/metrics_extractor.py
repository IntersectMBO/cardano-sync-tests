import datetime
import heapq
import itertools
import os
import pathlib as pl
import re
import shutil
import subprocess
import typing as tp


def merge_sorted_unique(*iterables: tp.Iterable) -> list:
    """Merge and sort multiple sorted iterables while removing duplicates."""
    return [key for key, _ in itertools.groupby(heapq.merge(*iterables))]


def filtered_log_fd(log_file: pl.Path, use_rg: bool = False) -> subprocess.Popen:
    """Run rg or grep to filter log lines and return a file descriptor for reading."""
    if use_rg:
        cmd = ["rg", "cardano\\.node\\.resources|new tip", str(log_file)]
    else:
        cmd = ["grep", "-E", "cardano\\.node\\.resources|new tip", str(log_file)]

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,  # Suppress errors (e.g., if file is missing)
        text=True,  # Ensures output is treated as text, not bytes
    )

    return process


def get_data_from_logs(log_file: pl.Path) -> dict[str, dict]:
    """Extract relevant data from the log file and return a dictionary."""
    chunk_size = 512 * 1024  # 512 KB
    tip_details_dict: dict[datetime.datetime, int] = {}
    heap_ram_details_dict: dict[datetime.datetime, float] = {}
    rss_ram_details_dict: dict[datetime.datetime, float] = {}
    centi_cpu_dict: dict[datetime.datetime, float] = {}
    cpu_details_dict: dict[datetime.datetime, float] = {}
    logs_details_dict: dict[str, dict[str, tp.Any]] = {}

    timestamp_pattern = re.compile(r"\d{4}-\d{2}-\d{2} \d{1,2}:\d{1,2}:\d{1,2}")
    heap_pattern = re.compile(r'"Heap",Number ([-+]?\d+\.?\d*(?:[Ee][-+]?\d+)?)')
    rss_pattern = re.compile(r'"RSS",Number ([-+]?\d+\.?\d*(?:[Ee][-+]?\d+)?)')
    centi_cpu_pattern = re.compile(r'"CentiCpu",Number (\d+\.\d+)')

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

    # Filter the log file with 'rg' (ripgrep) if available
    if shutil.which("rg"):
        with filtered_log_fd(log_file, use_rg=True) as process:
            if process.stdout:
                _process_log_file(infile=process.stdout)
    # If 'rg' is not available, check if 'grep' is available
    if not centi_cpu_dict and shutil.which("grep"):
        with filtered_log_fd(log_file) as process:
            if process.stdout:
                _process_log_file(infile=process.stdout)
    # If neither 'rg' nor 'grep' is available, read the log file directly without filtering
    if not centi_cpu_dict:
        with open(log_file, encoding="utf-8") as infile:
            _process_log_file(infile=infile)

    # Compute CPU load percentage per core
    no_of_cpu_cores = os.cpu_count() or 1
    timestamps_list = list(centi_cpu_dict)

    for prev_timestamp, curr_timestamp in zip(timestamps_list, timestamps_list[1:]):
        prev_value = centi_cpu_dict[prev_timestamp]
        curr_value = centi_cpu_dict[curr_timestamp]

        # Compute CPU load percentage over elapsed time
        cpu_load_percent = (curr_value - prev_value) / (
            curr_timestamp - prev_timestamp
        ).total_seconds()
        cpu_details_dict[curr_timestamp] = cpu_load_percent / no_of_cpu_cores

    # Collect all unique timestamps from different dictionaries
    all_timestamps_list = merge_sorted_unique(tip_details_dict, cpu_details_dict)

    # Populate logs_details_dict with merged data
    tip = -1
    for timestamp in all_timestamps_list:
        # Use the last known tip if no new tip is available
        tip = tip_details_dict.get(timestamp, tip)
        if tip == -1:
            continue
        cpu = cpu_details_dict.get(timestamp)
        if cpu is None:
            continue
        logs_details_dict[str(timestamp)] = {
            "tip": tip,
            "heap_ram": heap_ram_details_dict.get(timestamp, 0.0),
            "rss_ram": rss_ram_details_dict.get(timestamp, 0.0),
            "cpu": cpu,
        }

    return logs_details_dict
