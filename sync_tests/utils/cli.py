import dataclasses
import logging
import pathlib as pl
import re
import subprocess
import time

from sync_tests.utils import exceptions

LOGGER = logging.getLogger(__name__)

SPECIAL_ARG_CHARS_RE = re.compile("[^A-Za-z0-9/._-]")


@dataclasses.dataclass(frozen=True)
class CLIOut:
    stdout: bytes
    stderr: bytes


def _format_cli_args(cli_args: list[str]) -> str:
    """Format CLI arguments for logging.

    Quote arguments with spaces and other "special" characters in them.

    Args:
        cli_args: List of CLI arguments.
    """
    processed_args = []
    for arg in cli_args:
        arg_p = f'"{arg}"' if SPECIAL_ARG_CHARS_RE.search(arg) else arg
        processed_args.append(arg_p)
    return " ".join(processed_args)


def cli(
    cli_args: list[str],
    timeout: float | None = None,
) -> CLIOut:
    """Run the `cardano-cli` command.

    Args:
        cli_args: A list of arguments for cardano-cli.
        timeout: A timeout for the command, in seconds (optional).

    Returns:
        CLIOut: A data container containing command stdout and stderr.
    """
    cli_args_strs = [str(arg) for arg in cli_args]

    cmd_str = _format_cli_args(cli_args=cli_args_strs)
    LOGGER.debug("Running `%s`", cmd_str)

    # Re-run the command when running into
    # Network.Socket.connect: <socket: X>: resource exhausted (Resource temporarily unavailable)
    # or
    # MuxError (MuxIOException writev: resource vanished (Broken pipe)) "(sendAll errored)"
    for __ in range(3):
        retcode = None
        with subprocess.Popen(cli_args_strs, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as p:
            stdout, stderr = p.communicate(timeout=timeout)
            retcode = p.returncode

        if retcode == 0:
            break

        stderr_dec = stderr.decode()
        err_msg = (
            f"An error occurred running a CLI command `{cmd_str}` on path "
            f"`{pl.Path.cwd()}`: {stderr_dec}"
        )
        if "resource exhausted" in stderr_dec or "resource vanished" in stderr_dec:
            LOGGER.error(err_msg)
            time.sleep(0.4)
            continue
        raise exceptions.SyncError(err_msg)
    else:
        raise exceptions.SyncError(err_msg)

    return CLIOut(stdout or b"", stderr or b"")
