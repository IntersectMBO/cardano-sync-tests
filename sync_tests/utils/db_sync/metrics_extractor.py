"""Extract metrics from db-sync log files.

This module extracts epoch timing, block insertion, and epoch boundary metrics
from db-sync log files, similar to how metrics_extractor.py extracts node metrics.
"""

import datetime
import re
import shutil
import subprocess
import typing as tp
from pathlib import Path


def filtered_log_fd(log_file: Path, use_rg: bool = False) -> subprocess.Popen:
    """Run rg or grep to filter log lines and return a file descriptor for reading.

    Args:
        log_file: Path to the log file to filter.
        use_rg: If True, use ripgrep (rg), otherwise use grep.

    Returns:
        A subprocess.Popen object with stdout pipe for reading filtered lines.
    """
    # Pattern matches:
    # - "Insert.*Block.*epoch" - block insertions
    # - "Starting epoch" - epoch boundaries
    # - "Statistics for Epoch" - epoch statistics header
    # - "This epoch took" - epoch timing
    # - "Inserted epoch.*from updateEpochWhenSyncing" - epoch completion
    pattern = (
        r"Insert.*Block.*epoch|Starting epoch|Statistics for Epoch|"
        r"This epoch took|Inserted epoch.*from updateEpochWhenSyncing"
    )

    cmd = ["rg", pattern, str(log_file)] if use_rg else ["grep", "-E", pattern, str(log_file)]

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,  # Suppress errors (e.g., if file is missing)
        text=True,  # Ensures output is treated as text, not bytes
    )

    return process


def _parse_time_duration(time_str: str) -> float:
    """Parse a time duration string like '00:00:01.30' into seconds.

    Args:
        time_str: Time string in format HH:MM:SS.ff (fractional seconds)

    Returns:
        Duration in seconds as a float.
    """
    parts = time_str.split(":")
    if len(parts) != 3:
        return 0.0

    hours = int(parts[0])
    minutes = int(parts[1])
    seconds_parts = parts[2].split(".")
    seconds = int(seconds_parts[0])
    # The decimal part is fractional seconds (e.g., "30" means 0.30 seconds, not microseconds)
    fractional_seconds = float("0." + seconds_parts[1]) if len(seconds_parts) > 1 else 0.0

    total_seconds = hours * 3600 + minutes * 60 + seconds + fractional_seconds
    return total_seconds


def get_db_sync_data_from_logs(log_file: Path) -> dict[str, tp.Any]:
    """Extract epoch timing and block insertion metrics from db-sync log file.

    This function extracts:
    - Epoch timings (start time, end time, duration, blocks inserted)
    - Block insertions (timestamp, epoch, slot, block number, era type)
    - Epoch details (aggregated statistics per epoch)

    Args:
        log_file: Path to the db-sync log file.

    Returns:
        Dictionary with keys:
        - "epoch_timings": Dict mapping epoch number to start/end times and counts
        - "block_insertions": List of dicts with timestamp, epoch, slot, block, era
        - "epoch_details": Dict mapping epoch number to aggregated stats
    """
    chunk_size = 512 * 1024  # 512 KB

    # Data structures for collecting metrics
    epoch_timings: dict[int, dict[str, tp.Any]] = {}
    block_insertions: list[dict[str, tp.Any]] = []
    epoch_details: dict[int, dict[str, tp.Any]] = {}

    # Track current epoch being processed
    current_epoch: int | None = None
    current_epoch_start_time: datetime.datetime | None = None
    current_epoch_blocks: int = 0
    last_timestamp: datetime.datetime | None = None

    # Regex patterns
    timestamp_pattern = re.compile(r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{2}) UTC\]")
    block_insert_pattern = re.compile(
        r"Insert (Alonzo|Babbage|Conway|Byron|Shelley|Mary|Allegra) "
        r"Block: epoch (\d+), slot (\d+), block (\d+)"
    )
    starting_epoch_pattern = re.compile(r"Starting epoch (\d+)")
    statistics_epoch_pattern = re.compile(r"Statistics for Epoch (\d+)")
    epoch_took_pattern = re.compile(r"This epoch took: ([\d:\.]+) to process\.")
    epoch_completed_pattern = re.compile(r"Inserted epoch (\d+) from updateEpochWhenSyncing")

    def _process_log_line(line: str) -> None:
        """Extract relevant data from a log line and update dictionaries."""
        nonlocal current_epoch, current_epoch_start_time, current_epoch_blocks, last_timestamp

        # Extract timestamp if present
        timestamp_match = timestamp_pattern.search(line)
        timestamp: datetime.datetime | None = None

        if timestamp_match:
            try:
                timestamp = datetime.datetime.strptime(
                    timestamp_match.group(1), "%Y-%m-%d %H:%M:%S.%f"
                ).replace(tzinfo=datetime.timezone.utc)
                last_timestamp = timestamp  # Update last seen timestamp
            except ValueError:
                # Try without microseconds
                try:
                    timestamp = datetime.datetime.strptime(
                        timestamp_match.group(1), "%Y-%m-%d %H:%M:%S"
                    ).replace(tzinfo=datetime.timezone.utc)
                    last_timestamp = timestamp  # Update last seen timestamp
                except ValueError:
                    pass

        # Use last timestamp if current line doesn't have one (for lines like "This epoch took:")
        if timestamp is None:
            timestamp = last_timestamp

        # Skip processing if we still don't have a timestamp and the pattern needs one.
        if timestamp is None:
            # Check for patterns that might not have timestamps but are still useful
            took_match = epoch_took_pattern.search(line)
            if took_match and current_epoch is not None and last_timestamp is not None:
                # Use last timestamp for "This epoch took" lines
                duration_str = took_match.group(1)
                duration_sec = _parse_time_duration(duration_str)

                if current_epoch not in epoch_timings:
                    start_time = (
                        current_epoch_start_time.isoformat() if current_epoch_start_time else None
                    )
                    epoch_timings[current_epoch] = {
                        "start_time": start_time,
                        "end_time": last_timestamp.isoformat(),
                        "duration_sec": duration_sec,
                        "blocks_count": current_epoch_blocks,
                    }
                else:
                    epoch_timings[current_epoch]["end_time"] = last_timestamp.isoformat()
                    epoch_timings[current_epoch]["duration_sec"] = duration_sec
                    epoch_timings[current_epoch]["blocks_count"] = current_epoch_blocks
            return

        # Check for block insertion
        block_match = block_insert_pattern.search(line)
        if block_match:
            era = block_match.group(1)
            epoch = int(block_match.group(2))
            slot = int(block_match.group(3))
            block = int(block_match.group(4))

            block_insertions.append(
                {
                    "timestamp": timestamp.isoformat(),
                    "epoch": epoch,
                    "slot": slot,
                    "block": block,
                    "era": era,
                }
            )

            # Update block count for current epoch
            if epoch == current_epoch:
                current_epoch_blocks += 1
            elif epoch not in epoch_details:
                # Initialize epoch details if not already tracked
                epoch_details[epoch] = {"blocks_count": 1}
            else:
                epoch_details[epoch]["blocks_count"] = (
                    epoch_details[epoch].get("blocks_count", 0) + 1
                )

        # Check for "Starting epoch X"
        starting_match = starting_epoch_pattern.search(line)
        if starting_match:
            epoch = int(starting_match.group(1))
            current_epoch = epoch
            current_epoch_start_time = timestamp
            current_epoch_blocks = 0

            if epoch not in epoch_timings:
                epoch_timings[epoch] = {
                    "start_time": timestamp.isoformat(),
                    "end_time": None,
                    "duration_sec": None,
                    "blocks_count": 0,
                }
            else:
                epoch_timings[epoch]["start_time"] = timestamp.isoformat()

        # Check for "Statistics for Epoch X" (marks the end of epoch processing)
        stats_match = statistics_epoch_pattern.search(line)
        if stats_match:
            epoch = int(stats_match.group(1))
            current_epoch = epoch

        # Check for "This epoch took: HH:MM:SS to process."
        # (This is handled in the early return section above for lines without timestamps)
        took_match = epoch_took_pattern.search(line)
        if took_match and current_epoch is not None and timestamp is not None:
            duration_str = took_match.group(1)
            duration_sec = _parse_time_duration(duration_str)

            if current_epoch not in epoch_timings:
                start_time = (
                    current_epoch_start_time.isoformat() if current_epoch_start_time else None
                )
                epoch_timings[current_epoch] = {
                    "start_time": start_time,
                    "end_time": timestamp.isoformat(),
                    "duration_sec": duration_sec,
                    "blocks_count": current_epoch_blocks,
                }
            else:
                epoch_timings[current_epoch]["end_time"] = timestamp.isoformat()
                epoch_timings[current_epoch]["duration_sec"] = duration_sec
                epoch_timings[current_epoch]["blocks_count"] = current_epoch_blocks

        # Check for "Inserted epoch X from updateEpochWhenSyncing"
        completed_match = epoch_completed_pattern.search(line)
        if completed_match:
            epoch = int(completed_match.group(1))
            if epoch not in epoch_timings:
                epoch_timings[epoch] = {
                    "start_time": None,
                    "end_time": timestamp.isoformat(),
                    "duration_sec": None,
                    "blocks_count": 0,
                }
            elif epoch_timings[epoch]["end_time"] is None:
                epoch_timings[epoch]["end_time"] = timestamp.isoformat()

    def _process_log_file(infile: tp.IO) -> None:
        """Process the log file in chunks to handle large logs efficiently."""
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
    elif shutil.which("grep"):
        with filtered_log_fd(log_file, use_rg=False) as process:
            if process.stdout:
                _process_log_file(infile=process.stdout)
    # If neither 'rg' nor 'grep' is available, read the log file directly without filtering
    else:
        with open(log_file, encoding="utf-8") as infile:
            _process_log_file(infile=infile)

    # Finalize epoch details with timing information
    for epoch_no, timing_info in epoch_timings.items():
        if epoch_no not in epoch_details:
            epoch_details[epoch_no] = {}
        epoch_details[epoch_no].update(
            {
                "start_time": timing_info.get("start_time"),
                "end_time": timing_info.get("end_time"),
                "duration_sec": timing_info.get("duration_sec"),
                "blocks_count": timing_info.get("blocks_count", 0),
            }
        )

    return {
        "epoch_timings": epoch_timings,
        "block_insertions": block_insertions,
        "epoch_details": epoch_details,
    }
