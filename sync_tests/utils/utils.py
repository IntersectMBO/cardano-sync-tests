import json
import os
import platform
import zipfile
from datetime import datetime
from typing import NamedTuple

import psutil

from colorama import Fore, Style



class CLIOut(NamedTuple):
    stdout: bytes
    stderr: bytes


def print_message(message: str, type: str = "info"):
    """
    Print a message to the logs with color coding.

    Attributes:
        message (str): The message to print.
        type (str): The message level. Options are:
                     "ok", "info", "warn", "info_warn", "error".
                     Default is "info".
    """
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
    # dt1 and dt2 should be datetime types
    timedelta = dt2 - dt1
    return int(timedelta.days * 24 * 3600 + timedelta.seconds)


def seconds_to_time(seconds_val):
    mins, secs = divmod(seconds_val, 60)
    hour, mins = divmod(mins, 60)
    return "%d:%02d:%02d" % (hour, mins, secs)


def get_os_type():
    return [platform.system(), platform.release(), platform.version()]


def get_total_ram_in_GB():
    return int(psutil.virtual_memory().total / 1000000000)


def get_current_date_time():
    now = datetime.now()
    return now.strftime("%d/%m/%Y %H:%M:%S")


def print_file_content(file_name: str) -> None:
    try:
        with open(file_name, 'r') as file:
            content = file.read()
            print(content)
    except FileNotFoundError:
        print(f"File '{file_name}' not found.")
    except Exception as e:
        print(f"An error occurred while reading the file: {e}")


def list_absolute_file_paths(directory):
    files_paths = []
    for dirpath,_,filenames in os.walk(directory):
        for f in filenames:
            abs_filepath = os.path.abspath(os.path.join(dirpath, f))
            files_paths.append(abs_filepath)
    return files_paths


def get_directory_size(start_path='.'):
    # returns directory size in bytes
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def zip_file(archive_name, file_name):
    with zipfile.ZipFile(archive_name, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zip:
        zip.write(file_name)


def delete_file(file_path):
    # file_path should be a Path (pathlib object)
    try:
        file_path.unlink()
    except OSError as e:
        print_message(type="error", message=f"Error: {file_path} : {e.strerror}")


def load_json_files():
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    schema_path = os.path.join(base_path, 'schemas', 'expected_db_schema.json')
    indexes_path = os.path.join(base_path, 'schemas', 'expected_db_indexes.json')

    with open(schema_path, 'r') as schema_file:
        expected_db_schema = json.load(schema_file)

    with open(indexes_path, 'r') as indexes_file:
        expected_db_indexes = json.load(indexes_file)

    return expected_db_schema, expected_db_indexes


def get_arg_value(args, key, default=None):
    value = vars(args).get(key, default)
    if isinstance(value, str):
        value = value.strip()
    if key == "db_sync_start_options" and value == "--none":
        return ''
    return value