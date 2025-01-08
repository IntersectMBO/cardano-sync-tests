import json
import os
import platform
import shlex
import subprocess
import zipfile
from datetime import datetime
from typing import NamedTuple

import psutil
import logging

from colorama import Fore, Style

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class CLIOut(NamedTuple):
    stdout: bytes
    stderr: bytes


def print_message(message: str, type: str = "info"):
    """Print a message to the logs with color coding."""
    colors = {
        "ok": Fore.GREEN,
        "info": Fore.BLUE,
        "warn": Fore.YELLOW,
        "info_warn": Fore.LIGHTMAGENTA_EX,
        "error": Fore.RED
    }
    color = colors.get(type, Fore.BLUE)  # Default to 'info' if level is invalid
    print(color + f"{message}", Style.RESET_ALL, flush=True)


def get_os_type():
    """Retrieve the operating system type, release, and version."""
    return [platform.system(), platform.release(), platform.version()]


def get_total_ram_in_GB():
    """Get the total RAM size in gigabytes."""
    return int(psutil.virtual_memory().total / 1_000_000_000)


def print_file_content(file_name: str) -> None:
    """Print the content of a file."""
    try:
        with open(file_name, 'r') as file:
            content = file.read()
            print(content)
    except FileNotFoundError:
        logging.error(f"File '{file_name}' not found.")
    except Exception as e:
        logging.error(f"An error occurred while reading the file: {e}")


def get_directory_size(start_path='.'):
    """Calculate the total size of all files in a directory."""
    # returns directory size in bytes
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def zip_file(archive_name, file_name):
    """Compress a file into a zip archive."""
    try:
        with zipfile.ZipFile(archive_name, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zip:
            zip.write(file_name)
        logging.info(f"File '{file_name}' successfully zipped as '{archive_name}'.")
    except Exception as e:
        logging.error(f"Error while zipping file: {e}")


def delete_file(file_path):
    """Delete a file from the file system."""
    try:
        file_path.unlink()
        logging.info(f"File '{file_path}' deleted successfully.")
    except OSError as e:
        logging.error(f"Error deleting file '{file_path}': {e.strerror}")


def load_json_files():
    """Load JSON files for database schema and indexes."""
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    schema_path = os.path.join(base_path, 'schemas', 'expected_db_schema.json')
    indexes_path = os.path.join(base_path, 'schemas', 'expected_db_indexes.json')

    with open(schema_path, 'r') as schema_file:
        expected_db_schema = json.load(schema_file)

    with open(indexes_path, 'r') as indexes_file:
        expected_db_indexes = json.load(indexes_file)

    return expected_db_schema, expected_db_indexes


def get_arg_value(args, key, default=None):
    """Retrieve the value of a specific argument from an arguments object."""
    value = vars(args).get(key, default)
    if isinstance(value, str):
        value = value.strip()
    if key == "db_sync_start_options" and value == "--none":
        return ''
    return value


def execute_command(command):
    """Executes a shell command and logs its output and errors."""
    logging.info(f"--- Execute command {command}")
    try:
        cmd = shlex.split(command)
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
        outs, errors = process.communicate(timeout=3600)

        if errors:
            logging.info(f"Warnings or Errors: {errors}")

        logging.info(f"Output of command: {command} : {outs}")
        exit_code = process.returncode

        if exit_code != 0:
            logging.error(f"Command {command} returned exit code: {exit_code}")
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        logging.error(f"Command {command} returned exception: {e}")
        raise
