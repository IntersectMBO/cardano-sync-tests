"""Extract performance metrics from cardano-node log files."""

from __future__ import annotations

import datetime
import heapq
import itertools
import logging
import pathlib as pl
import re
import typing as tp

from sync_tests.utils.logs.filtering import MarkerReachedError
from sync_tests.utils.logs.filtering import process_filtered_log

LOGGER = logging.getLogger(__name__)


def merge_sorted_unique(*iterables: tp.Iterable) -> list:
    """Merge and sort multiple sorted iterables while removing duplicates."""
    return [key for key, _ in itertools.groupby(heapq.merge(*iterables))]


def get_data_from_logs(log_file: pl.Path, stop_marker: str | None = None) -> dict[str, dict]:
    """Extract relevant data from the log file and return a dictionary.

    Args:
        log_file: Path to the log file.
        stop_marker: Optional marker string; parsing stops at the first line that
            contains it, so data logged after the marker is ignored. When unset,
            the whole log file is parsed.
    """
    chunk_size = 512 * 1024  # 512 KB
    tip_details_dict: dict[datetime.datetime, int] = {}
    heap_ram_details_dict: dict[datetime.datetime, float] = {}
    rss_ram_details_dict: dict[datetime.datetime, float] = {}
    centi_cpu_dict: dict[datetime.datetime, float] = {}
    cpu_ticks_dict: dict[datetime.datetime, float] = {}
    cpu_details_dict: dict[datetime.datetime, float] = {}
    logs_details_dict: dict[str, dict[str, tp.Any]] = {}
    marker_hit = False

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
        # Stop at the marker; anything logged after it is out of the measured window
        if stop_marker and stop_marker in line:
            raise MarkerReachedError

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
            ).replace(tzinfo=datetime.UTC)
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
            ).replace(tzinfo=datetime.UTC)
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
            ).replace(tzinfo=datetime.UTC)
            tip_details_dict[timestamp] = int(line.split(" at slot ", 1)[1])

    def _process_log_file(infile: tp.IO) -> None:
        # Read the file in chunks to handle large logs efficiently
        nonlocal marker_hit
        incomplete_line = ""
        try:
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
        except MarkerReachedError:
            marker_hit = True

    def _no_cpu_data() -> bool:
        return not centi_cpu_dict and not cpu_ticks_dict

    filter_pattern = r"cardano\.node\.resources|Resources:|new tip"
    if stop_marker:
        filter_pattern = f"{filter_pattern}|{re.escape(stop_marker)}"

    process_filtered_log(
        log_file=log_file,
        pattern=filter_pattern,
        handler=_process_log_file,
        tool="rg",
    )

    # Parsing that ended at the marker legitimately sees no CPU data; retrying then
    # would re-walk the log only to stop at the same marker again.
    if not marker_hit and _no_cpu_data():
        process_filtered_log(
            log_file=log_file,
            pattern=filter_pattern,
            handler=_process_log_file,
            tool="grep",
        )
    # If neither 'rg' nor 'grep' is available, read the log file directly without filtering
    if not marker_hit and _no_cpu_data():
        with open(log_file, encoding="utf-8") as infile:
            _process_log_file(infile=infile)

    if marker_hit and _no_cpu_data() and not tip_details_dict:
        LOGGER.warning(
            "No node metrics found before stop marker %r in %s; the marker may predate "
            "the measured data (e.g. left over from an earlier run)",
            stop_marker,
            log_file,
        )

    # Both sources report the same counter in centiseconds of CPU time: the legacy
    # tracing renders it as "CentiCpu", the new tracing as "Cpu Ticks".
    cpu_source = centi_cpu_dict or cpu_ticks_dict
    for prev_timestamp, curr_timestamp in itertools.pairwise(cpu_source):
        prev_value = cpu_source[prev_timestamp]
        curr_value = cpu_source[curr_timestamp]
        elapsed = (curr_timestamp - prev_timestamp).total_seconds()
        if elapsed <= 0:
            continue

        # Compute CPU load percentage over elapsed time (no per-core split;
        # centiseconds per second is already a percentage).
        cpu_load_percent = (curr_value - prev_value) / elapsed
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
