import argparse
import contextlib
import json
import logging
import os
import pathlib as pl
import platform
import shlex
import stat
import subprocess
import typing as tp
import zipfile

import psutil
from colorama import Fore
from colorama import Style

LOGGER = logging.getLogger(__name__)


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


def print_file_content(file_name: str) -> None:
    """Print the content of a file."""
    try:
        with open(file_name) as file:
            content = file.read()
            print(content)
    except FileNotFoundError:
        LOGGER.exception(f"File '{file_name}' not found.")
    except Exception:
        LOGGER.exception("An error occurred while reading the file")


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
    try:
        with zipfile.ZipFile(
            archive_name, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
        ) as zip:
            zip.write(file_name)
        LOGGER.info(f"File '{file_name}' successfully zipped as '{archive_name}'.")
    except Exception:
        LOGGER.exception("Error while zipping file")


def delete_file(file_path: pl.Path) -> None:
    """Delete a file from the file system."""
    try:
        file_path.unlink()
        LOGGER.info(f"File '{file_path}' deleted successfully.")
    except OSError as e:
        LOGGER.exception(f"Error deleting file '{file_path}': {e.strerror}")


def load_json_files() -> tuple[dict, dict]:
    """Load JSON files for database schema and indexes."""
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    schema_path = os.path.join(base_path, "schemas", "expected_db_schema.json")
    indexes_path = os.path.join(base_path, "schemas", "expected_db_indexes.json")

    with open(schema_path) as schema_file:
        expected_db_schema = json.load(schema_file)

    with open(indexes_path) as indexes_file:
        expected_db_indexes = json.load(indexes_file)

    return expected_db_schema, expected_db_indexes


def get_arg_value(args: argparse.Namespace, key: str, default: tp.Any | None = None) -> tp.Any:
    """Retrieve the value of a specific argument from an arguments object."""
    value = vars(args).get(key, default)
    if isinstance(value, str):
        value = value.strip()
    if key == "db_sync_start_options" and value == "--none":
        return ""
    return value


def execute_command(command: str) -> None:
    """Execute a shell command and logs its output and errors."""
    LOGGER.info(f"--- Execute command {command}")
    try:
        cmd = shlex.split(command)
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8"
        )
        outs, errors = process.communicate(timeout=3600)

        if errors:
            LOGGER.info(f"Warnings or Errors: {errors}")

        LOGGER.info(f"Output of command: {command} : {outs}")
        exit_code = process.returncode

        if exit_code != 0:
            LOGGER.error(f"Command {command} returned exit code: {exit_code}")
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        LOGGER.exception("Command {command} returned exception")
        raise


def update_json_file(file_path: pl.Path, updates: dict) -> None:
    """Read a JSON file, updates it with the provided dictionary, and writes it back."""
    with open(file_path) as json_file:
        data = json.load(json_file)

    data.update(updates)

    with open(file_path, "w") as json_file:
        json.dump(data, json_file, indent=2)


@contextlib.contextmanager
def temporary_chdir(path: pl.Path) -> tp.Iterator[None]:
    prev_cwd = pl.Path.cwd()  # Store the current working directory
    try:
        os.chdir(path)  # Change to the new directory
        yield
    finally:
        os.chdir(prev_cwd)  # Restore the original working directory


def make_executable(path: pl.Path) -> None:
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def manage_process(proc_name: str, action: str) -> psutil.Process:
    """Manage a process by retrieving, terminating, or killing based on the action specified."""
    for proc in psutil.process_iter():
        if proc_name in proc.name():
            if action == "get":
                return proc
            if action == "terminate":
                logging.info(f"Attempting to terminate the {proc_name} process - {proc}")
                proc.terminate()
                proc.wait(timeout=30)  # Wait for the process to terminate
                if proc.is_running():
                    logging.warning(
                        f"Termination failed, forcefully killing the {proc_name} process - {proc}"
                    )
                    proc.kill()
            else:
                msg = "Action must be 'get' or 'terminate'"
                raise ValueError(msg)
    return None
