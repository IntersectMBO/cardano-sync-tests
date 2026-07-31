"""Shared helper functions used across sync-tests utilities."""

from __future__ import annotations

import argparse
import collections
import hashlib
import json
import logging
import os
import pathlib as pl
import platform
import shlex
import shutil
import stat
import subprocess
import time
import typing as tp
import zipfile

import psutil
import requests
from colorama import Fore
from colorama import Style

LOGGER = logging.getLogger(__name__)

DEFAULT_HTTP_TIMEOUT_SECONDS = 30

_NIX_BUILD_LOG_ENV = "SYNC_TESTS_NIX_BUILD_LOG"
_DEFAULT_NIX_BUILD_LOG = "test_workdir/nix_build.log"
_nix_build_log_fd: int | None = None


def _nix_build_log_enabled() -> bool:
    return bool(os.getenv("GITHUB_ACTIONS"))


def _get_nix_build_log_fd() -> int:
    """Return append fd for nix/execute_command output (GitHub Actions only)."""
    global _nix_build_log_fd
    if _nix_build_log_fd is None:
        path = pl.Path(os.getenv(_NIX_BUILD_LOG_ENV, _DEFAULT_NIX_BUILD_LOG))
        path.parent.mkdir(parents=True, exist_ok=True)
        _nix_build_log_fd = os.open(
            path,
            os.O_WRONLY | os.O_APPEND | os.O_CREAT,
            0o644,
        )
    return _nix_build_log_fd


def _emit_command_log_line(line: str) -> None:
    """Record subprocess output for CI artifacts.

    On GitHub Actions, append to ``nix_build.log``. Pytest capture and
    ``--log-cli-level=WARNING`` prevent this output from reaching the shell
    redirect that writes ``ci_step.log``, so a dedicated append log is required.

    Locally, stream through normal logging instead.
    """
    if _nix_build_log_enabled():
        os.write(
            _get_nix_build_log_fd(),
            (f"{line}\n").encode("utf-8", errors="replace"),
        )
    else:
        LOGGER.info("%s", line)


def print_message(message: str, type: str = "info") -> None:
    """Print a message to the logs with color coding."""
    colors = {
        "ok": Fore.GREEN,
        "info": Fore.BLUE,
        "warn": Fore.YELLOW,
        "info_warn": Fore.LIGHTMAGENTA_EX,
        "error": Fore.RED,
    }
    color = colors.get(type, Fore.BLUE)  # Default to 'info' if level is invalid
    print(color + f"{message}", Style.RESET_ALL, flush=True)


def get_os_type() -> list[str]:
    """Retrieve the operating system type, release, and version."""
    return [platform.system(), platform.release(), platform.version()]


def get_total_ram_in_gb() -> int:
    """Get the total RAM size in gigabytes."""
    return int(psutil.virtual_memory().total / 1_000_000_000)


def get_directory_size(start_path: str | pl.Path = ".") -> int:
    """Calculate the total size of all files in a directory."""
    # returns directory size in bytes
    total_size = 0
    for dirpath, _dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def zip_file(archive_name: str, file_name: str | pl.Path) -> None:
    """Compress a file into a zip archive."""
    file_path = pl.Path(file_name)
    if not file_path.exists():
        LOGGER.warning("Skipping zip: source file does not exist: %s", file_name)
        return
    try:
        with zipfile.ZipFile(
            archive_name, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
        ) as zf:
            zf.write(file_name, pl.Path(file_name).name)
        LOGGER.info("File '%s' successfully zipped as '%s'.", file_name, archive_name)
    except (OSError, zipfile.BadZipFile):
        LOGGER.exception("Error while zipping file")


def zip_files(archive_name: str, file_names: list[str | pl.Path]) -> None:
    """Compress multiple files into a single zip archive, skipping missing ones.

    Args:
        archive_name: Output archive filename.
        file_names: List of file paths to include; missing files are skipped.
    """
    files_to_zip = [pl.Path(f) for f in file_names if pl.Path(f).exists()]
    if not files_to_zip:
        LOGGER.warning("Skipping zip: none of the source files exist for %s", archive_name)
        return
    try:
        with zipfile.ZipFile(
            archive_name, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
        ) as zf:
            for f in files_to_zip:
                zf.write(f, f.name)
        LOGGER.info("Zipped %s file(s) into '%s'.", len(files_to_zip), archive_name)
    except (OSError, zipfile.BadZipFile):
        LOGGER.exception("Error while zipping files into %s", archive_name)


_MONITOR_BUNDLE_LOG_NAMES = frozenset(
    {"monitor.log", "monitor_stderr.log", "oom.log", "postgres.log"}
)


def list_sync_log_files(workdir: pl.Path) -> list[pl.Path]:
    """Return workdir *.log files for sync_logs.zip, excluding monitor bundle logs."""
    return sorted(p for p in workdir.glob("*.log") if p.name not in _MONITOR_BUNDLE_LOG_NAMES)


def create_zip_bundle(archive_path: pl.Path, files: list[pl.Path], base_dir: pl.Path) -> None:
    """Create a zip bundle from provided files, skipping missing ones.

    Args:
        archive_path: Output .zip path.
        files: Files to include in the archive.
        base_dir: Base directory used to compute relative archive entry names.
    """
    files_to_zip = [f for f in files if f.exists()]
    if not files_to_zip:
        LOGGER.warning("Skipping zip bundle: none of the source files exist for %s", archive_path)
        return
    try:
        with zipfile.ZipFile(
            archive_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
        ) as zip_fp:
            for file_path in files_to_zip:
                arcname = (
                    file_path.relative_to(base_dir)
                    if file_path.is_relative_to(base_dir)
                    else file_path.name
                )
                zip_fp.write(file_path, arcname=str(arcname))
    except (OSError, zipfile.BadZipFile):
        LOGGER.exception("Error while creating zip bundle %s", archive_path)


def delete_file(file_path: pl.Path) -> None:
    """Delete a file from the file system."""
    try:
        file_path.unlink()
        LOGGER.info("File '%s' deleted successfully.", file_path)
    except OSError as e:
        LOGGER.exception("Error deleting file '%s': %s", file_path, e.strerror)


def get_arg_value(args: argparse.Namespace, key: str, default: tp.Any | None = None) -> tp.Any:
    """Retrieve the value of a specific argument from an arguments object."""
    value = vars(args).get(key, default)
    if isinstance(value, str):
        value = value.strip()
    if key == "db_sync_start_options" and value == "--none":
        return ""
    return value


def execute_command(
    command: str,
    cwd: str | pl.Path | None = None,
    *,
    check: bool = True,
) -> None:
    """Execute a shell command and log its output.

    Long commands (e.g. ``nix build``) stream stdout/stderr line-by-line so CI
    consoles do not appear hung. A bounded tail is kept for failure messages.

    Args:
        command: Full command line (parsed with :func:`shlex.split`).
        cwd: Working directory for the subprocess, or ``None`` for inherited cwd.
        check: If ``True`` (default), raise :class:`RuntimeError` when the process
            exits non-zero so callers do not continue after a failed ``nix build``
            or similar.
    """
    cwd_display = f" (cwd={cwd})" if cwd else ""
    _emit_command_log_line(f"--- Execute command {command}{cwd_display}")
    try:
        cmd = shlex.split(command)
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            errors="replace",
            cwd=str(cwd) if cwd else None,
        )
        merged_tail: collections.deque[str] = collections.deque(maxlen=400)
        assert process.stdout is not None
        for raw_line in process.stdout:
            line = raw_line.rstrip("\r\n")
            merged_tail.append(line)
            _emit_command_log_line(line)
        exit_code = process.wait()
        combined = "\n".join(merged_tail)

        _emit_command_log_line(f"Command finished (exit {exit_code}): {command}{cwd_display}")

        if exit_code != 0:
            LOGGER.error("Command %s returned exit code: %s", command, exit_code)
            if check:
                tail = combined.strip()[-6000:]
                parts = [f"Command failed (exit {exit_code}): {command}{cwd_display}"]
                if tail:
                    parts.append(f"output (tail):\n{tail}")
                hint = (
                    "\n\nIf stderr mentions Nix 2.15 vs 2.18: upgrade Nix on the CI agent "
                    "(e.g. `nix upgrade-nix` or install Nix ≥ 2.18) so `nix build` for "
                    "cardano-db-sync can evaluate current nixpkgs."
                )
                if "nixVersion" in combined or "Nix 2." in combined:
                    parts.append(hint)
                raise RuntimeError("\n".join(parts))
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        LOGGER.exception("Command %s returned exception", command)
        raise


def update_json_file(file_path: pl.Path, updates: dict) -> None:
    """Read a JSON file, updates it with the provided dictionary, and writes it back."""
    with open(file_path) as json_file:
        data = json.load(json_file)

    data.update(updates)

    with open(file_path, "w") as json_file:
        json.dump(data, json_file, indent=2)


def remove_json_keys(file_path: pl.Path, keys: list[str]) -> None:
    """Read a JSON file, remove specified keys if present, and write it back.

    Args:
        file_path: Path to the JSON file to modify.
        keys: List of top-level keys to remove from the JSON object.
    """
    with open(file_path) as json_file:
        data = json.load(json_file)

    for key in keys:
        data.pop(key, None)

    with open(file_path, "w") as json_file:
        json.dump(data, json_file, indent=2)


def make_executable(path: pl.Path) -> None:
    """Set executable bits on a file for owner, group, and others."""
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def manage_process(proc_name: str, action: str) -> psutil.Process | None:
    """Manage a process by retrieving or terminating a named process."""
    for proc in psutil.process_iter():
        try:
            proc_label = proc.name()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

        if proc_name in proc_label:
            if action == "get":
                return proc
            if action == "terminate":
                LOGGER.info("Attempting to terminate the %s process - %s", proc_name, proc)
                try:
                    proc.terminate()
                    proc.wait(timeout=30)
                    if proc.is_running():
                        LOGGER.warning(
                            "Termination failed, forcefully killing the %s process - %s",
                            proc_name,
                            proc,
                        )
                        proc.kill()
                except (psutil.NoSuchProcess, psutil.TimeoutExpired):
                    if proc.is_running():
                        LOGGER.warning(
                            "Termination timed out, forcefully killing the %s process - %s",
                            proc_name,
                            proc,
                        )
                        proc.kill()
                    continue
            else:
                msg = "Action must be 'get' or 'terminate'"
                raise ValueError(msg)
    return None


# utility functions from db sync
def export_env_var(name: str, value: tp.Any) -> None:
    """Export an environment variable with the given name and value.

    Args:
        name: The environment variable name.
        value: The value to set (will be converted to string).
    """
    os.environ[name] = str(value)


def make_tarfile(output_filename: str, source_dir: str) -> None:
    """Create a tar.gz archive of the specified source directory.

    Args:
        output_filename: The output archive filename (should end in .tar.gz).
        source_dir: The directory to archive.
    """
    shutil.make_archive(base_name=output_filename[:-7], format="gztar", root_dir=source_dir)


def write_json_to_file(file_path: str | pl.Path, data: dict | list) -> None:
    """Write data to a file in JSON format.

    Args:
        file_path: Path to the output JSON file.
        data: Dictionary or list to serialize as JSON.
    """
    file_path = pl.Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)


def manage_directory(dir_name: str, action: str, root: str = ".") -> str | None:
    """Manage a directory by creating or removing it based on the action specified.

    Args:
        dir_name: Name of the directory to manage.
        action: Either 'create' or 'remove'.
        root: Root path to use (default: current directory).

    Returns:
        str: Path to created directory if action is 'create'.
        None: If action is 'remove'.

    Raises:
        ValueError: If action is not 'create' or 'remove'.
    """
    path = pl.Path(f"{root}/{dir_name}")
    if action == "create":
        path.mkdir(parents=True, exist_ok=True)
        return str(path)
    if action == "remove":
        if path.exists():
            shutil.rmtree(path)
        return None
    msg = "Action must be either 'create' or 'remove'."
    raise ValueError(msg)


def get_file_sha256_sum(filepath: str | pl.Path) -> str:
    """Calculate and return the SHA-256 checksum of a file.

    Args:
        filepath: Path to the file to hash.

    Returns:
        str: Hexadecimal SHA-256 checksum.
    """
    with open(filepath, "rb") as f:
        return hashlib.file_digest(f, hashlib.sha256).hexdigest()


def request_with_retry(
    method: str,
    url: str,
    max_attempts: int = 3,
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    **kwargs: tp.Any,
) -> requests.Response:
    """Issue an HTTP request with a timeout and exponential-backoff retry.

    A hung remote endpoint must fail fast and retry rather than block a
    multi-hour sync-test job indefinitely.

    Args:
        method: HTTP method, e.g. "get" or "post".
        url: Request URL.
        max_attempts: Number of attempts before giving up.
        timeout: Per-request timeout in seconds.
        **kwargs: Forwarded to ``requests.request`` (headers, data, stream, ...).

    Returns:
        The ``requests.Response`` from the first successful attempt.

    Raises:
        requests.RequestException: If every attempt fails (connection error or timeout).
    """
    last_exc: requests.RequestException | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return requests.request(method, url, timeout=timeout, **kwargs)
        except requests.RequestException as exc:
            last_exc = exc
            if attempt == max_attempts:
                break
            backoff_secs = 2 ** (attempt - 1)
            LOGGER.warning(
                "%s %s failed on attempt %s/%s: %s. Retrying in %ss.",
                method.upper(),
                url,
                attempt,
                max_attempts,
                exc,
                backoff_secs,
            )
            time.sleep(backoff_secs)

    if last_exc is None:  # unreachable, satisfies type checking
        msg = "request_with_retry exhausted attempts without an exception"
        raise RuntimeError(msg)
    raise last_exc


def print_last_n_lines(file_path: str | pl.Path, n: int) -> None:
    """Print the last n lines from the specified file.

    Args:
        file_path: Path to the file to read.
        n: Number of lines to print from the end.
    """
    try:
        result = subprocess.run(
            ["tail", "-n", str(n), str(file_path)],
            stdout=subprocess.PIPE,
            check=False,
        )
        for line in result.stdout.decode("utf-8").strip().splitlines():
            LOGGER.info(line)
    except Exception:
        LOGGER.exception("Error reading last %s lines from %s", n, file_path)
