"""Shared helpers for opening filtered log streams."""

from __future__ import annotations

import pathlib as pl
import shutil
import subprocess
import typing as tp


class MarkerReachedError(Exception):
    """Raised by a line handler once a stop marker is seen in the log."""


def build_filter_command(tool: str, pattern: str, log_file: pl.Path) -> list[str]:
    """Build rg/grep command used to filter logs."""
    if tool == "rg":
        return ["rg", pattern, str(log_file)]
    if tool == "grep":
        return ["grep", "-E", pattern, str(log_file)]

    msg = f"Unsupported filter tool: {tool}"
    raise ValueError(msg)


def open_filtered_log_fd(
    log_file: pl.Path,
    pattern: str,
    tool: tp.Literal["auto", "rg", "grep"] = "auto",
) -> subprocess.Popen | None:
    """Open a filtered log stream using rg/grep if available.

    Args:
        log_file: Path to log file.
        pattern: Regex pattern string for rg/grep.
        tool: Preferred filter tool or ``"auto"``.

    Returns:
        A subprocess with stdout configured for reading, or ``None`` when the
        requested filter tool is unavailable.
    """
    selected_tool: str | None = None
    if tool == "auto":
        if shutil.which("rg"):
            selected_tool = "rg"
        elif shutil.which("grep"):
            selected_tool = "grep"
    elif shutil.which(tool):
        selected_tool = tool

    if selected_tool is None:
        return None

    cmd = build_filter_command(selected_tool, pattern, log_file)
    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )


def process_filtered_log(
    log_file: pl.Path,
    pattern: str,
    handler: tp.Callable[[tp.IO], None],
    tool: tp.Literal["auto", "rg", "grep"] = "auto",
) -> bool:
    """Run `handler` over a filtered log stream.

    The handler may stop reading early (e.g. on a stop marker); the filter tool is
    then terminated instead of being left to walk the rest of the log.

    Args:
        log_file: Path to log file.
        pattern: Regex pattern string for rg/grep.
        handler: Callable that consumes the filtered stream.
        tool: Preferred filter tool or ``"auto"``.

    Returns:
        ``True`` when the filter tool was available and the stream was consumed,
        ``False`` when no filter tool is available and nothing was read.
    """
    process = open_filtered_log_fd(log_file=log_file, pattern=pattern, tool=tool)
    if process is None:
        return False

    try:
        if process.stdout:
            handler(process.stdout)
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if process.stdout:
            process.stdout.close()

    return True
