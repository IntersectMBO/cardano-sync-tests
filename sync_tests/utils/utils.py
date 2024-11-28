import json
import os
import shlex
import shutil
import platform
import subprocess
import zipfile
from datetime import datetime
from typing import NamedTuple
from typing import List
from typing import Union
from pathlib import Path

import psutil
import time

from colorama import Fore, Style



class CLIOut(NamedTuple):
    stdout: bytes
    stderr: bytes



def execute_command(
    command: Union[str, list],
    ignore_fail: bool = False,
    shell: bool = False,
    timeout: int = 3600,
    log_output: bool = True,
) -> CLIOut:
    """
    Execute a shell command.

    Args:
        command (Union[str, list]): The command to run, as a string or a list of arguments.
        ignore_fail (bool): If True, do not raise an exception on non-zero exit codes.
        shell (bool): Whether to execute the command in a shell.
        timeout (int): Maximum time (in seconds) to wait for the command to complete.
        log_output (bool): If True, logs command output to the console.

    Returns:
        CLIOut: A NamedTuple containing stdout and stderr outputs.
    """
    # Prepare command
    cmd = command if isinstance(command, list) else (command if shell else shlex.split(command))
    print(f"--- Executing command: {cmd}")

    try:
        # Run command
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=shell, encoding='utf-8'
        )
        stdout, stderr = process.communicate(timeout=timeout)

        # Log outputs if enabled
        if log_output:
            if stdout:
                print(f"Output:\n{stdout}", flush=True)
            if stderr:
                print(f"Warnings or Errors:\n{stderr}", flush=True)

        # Check exit code
        if process.returncode != 0:
            if not ignore_fail:
                raise RuntimeError(f"Command '{command}' failed with exit code {process.returncode}: {stderr}")
            else:
                print(f"Command '{command}' failed but ignore_fail is set. Exit code: {process.returncode}")

        return CLIOut(stdout or "", stderr or "")

    except subprocess.TimeoutExpired as e:
        process.kill()
        print(f"Command '{command}' timed out after {timeout} seconds.")
        raise RuntimeError(f"Timeout expired for command: {command}") from e

    except subprocess.CalledProcessError as e:
        print(f"Command '{command}' encountered an error: {e}")
        raise


def cli_has(command: str) -> bool:
    """Check if a cardano-cli subcommand or argument is available.
    E.g. `cli_has("query leadership-schedule --next")`
    """
    err_str = ""
    try:
        run_command(command)
    except RuntimeError as err:
        err_str = str(err)
    else:
        return True

    cmd_err = err_str.split(":", maxsplit=1)[1].strip()
    return not cmd_err.startswith("Invalid")


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


def get_no_of_cpu_cores():
    return os.cpu_count()


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


def delete_file(file_path):
    # file_path should be a Path (pathlib object)
    try:
        file_path.unlink()
    except OSError as e:
        print_message(type="error", message=f"Error: {file_path} : {e.strerror}")


def load_json_files():
    schema_path = os.path.join(os.path.dirname(__file__), '..', 'schemas', 'expected_db_schema.json')
    indexes_path = os.path.join(os.path.dirname(__file__), '..', 'schemas', 'expected_db_indexes.json')

    with open(schema_path, 'r') as schema_file:
        expected_db_schema = json.load(schema_file)

    with open(indexes_path, 'r') as indexes_file:
        expected_db_indexes = json.load(indexes_file)

    return expected_db_schema, expected_db_indexes