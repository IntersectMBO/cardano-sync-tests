import json
import os
import platform
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


def date_diff_in_seconds(dt2, dt1):
    """Calculate the difference in seconds between two datetime objects."""
    timedelta = dt2 - dt1
    return int(timedelta.total_seconds())


def seconds_to_time(seconds_val):
    """Convert seconds into a formatted time string (HH:MM:SS)."""
    mins, secs = divmod(seconds_val, 60)
    hour, mins = divmod(mins, 60)
    return f"{hour}:{mins:02}:{secs:02}"


def get_os_type():
    """Retrieve the operating system type, release, and version."""
    return [platform.system(), platform.release(), platform.version()]


def get_total_ram_in_GB():
    """Get the total RAM size in gigabytes."""
    return int(psutil.virtual_memory().total / 1_000_000_000)


def get_current_date_time():
    """Get the current date and time as a formatted string."""
    now = datetime.now()
    return now.strftime("%d/%m/%Y %H:%M:%S")


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


def list_absolute_file_paths(directory):
    """List all absolute file paths in a given directory."""
    files_paths = []
    for dirpath,_,filenames in os.walk(directory):
        for f in filenames:
            abs_filepath = os.path.abspath(os.path.join(dirpath, f))
            files_paths.append(abs_filepath)
    return files_paths


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