import argparse
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


def execute_command(command: str, cwd: str | pl.Path | None = None) -> None:
    """Execute a shell command and logs its output and errors."""
    cwd_display = f" (cwd={cwd})" if cwd else ""
    LOGGER.info(f"--- Execute command {command}{cwd_display}")
    try:
        cmd = shlex.split(command)
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
            cwd=str(cwd) if cwd else None,
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
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def manage_process(proc_name: str, action: str) -> psutil.Process:
    """Manage a process by retrieving, terminating, or killing based on the action specified."""
    for proc in psutil.process_iter():
        try:
            proc_label = proc.name()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

        if proc_name in proc_label:
            if action == "get":
                return proc
            if action == "terminate":
                logging.info(f"Attempting to terminate the {proc_name} process - {proc}")
                try:
                    proc.terminate()
                    proc.wait(timeout=30)  # Wait for the process to terminate
                    if proc.is_running():
                        logging.warning(
                            "Termination failed, forcefully killing the %s process - %s",
                            proc_name,
                            proc,
                        )
                        proc.kill()
                except (psutil.NoSuchProcess, psutil.TimeoutExpired):
                    if proc.is_running():
                        logging.warning(
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
        LOGGER.exception(f"Error reading last {n} lines from {file_path}")
