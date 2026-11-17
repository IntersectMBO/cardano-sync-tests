#!/usr/bin/env python3
"""Bundle the results of one node sync run for upload to the tcache.

Usage:
    sync_tests/scripts/sync_results_zip.py --workdir DIR --output FILE

The zip holds `node_sync_results.json` and the run's sync markers and
progress files, at the zip root like the CI results artifact. The version,
start time and network the upload names the run by, all read from the
results file, are printed on one line separated by tabs.
"""

from __future__ import annotations

import argparse
import json
import logging
import pathlib as pl
import sys
import zipfile

LOGGER = logging.getLogger(__name__)

NODE_RESULTS = "node_sync_results.json"
EXTRA_FILES = ("sync_markers_{env}.json", "sync_progress_{env}.json")
# Not 2, which argparse uses for a bad command line.
NO_VERSION = 3
NO_NAME = 4


def get_args() -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(description="Bundle node sync results for the tcache.")
    parser.add_argument("--workdir", required=True, type=pl.Path, help="The sync run's workdir.")
    parser.add_argument("--output", required=True, type=pl.Path, help="The zip to write.")
    return parser.parse_args()


def bundle(workdir: pl.Path, environment: str, output: pl.Path) -> list[str]:
    """Write the zip and return the names of the files in it.

    Args:
        workdir: The sync run's workdir.
        environment: The network the run synced.
        output: The zip to write.

    Returns:
        The member names, the results file first.
    """
    files = [workdir / NODE_RESULTS]
    files.extend(
        workdir / name.format(env=environment)
        for name in EXTRA_FILES
        if (workdir / name.format(env=environment)).is_file()
    )
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in files:
            zf.write(path, arcname=path.name)
    return [path.name for path in files]


def run_name(workdir: pl.Path) -> tuple[str, str, str]:
    """Return the version, start time and network of the run.

    Args:
        workdir: The sync run's workdir.

    Returns:
        The `tag_no1`, `start_sync_time1` and `env` of the results file.

    Raises:
        KeyError: When one of them is missing. A sync of a node given by
            socket path records no version.
    """
    doc = json.loads((workdir / NODE_RESULTS).read_text(encoding="utf-8"))
    fields = {name: doc.get(name) for name in ("tag_no1", "start_sync_time1", "env")}
    missing = [name for name, value in fields.items() if not value]
    if missing:
        raise KeyError(", ".join(missing))
    return tuple(str(value) for value in fields.values())  # type: ignore[return-value]


def main() -> int:
    """Write the zip and print the run's name.

    Returns:
        0 when the zip is written, 3 when the results name no node version,
        4 when they name no start time or network.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = get_args()
    try:
        version, started, environment = run_name(args.workdir)
    except KeyError as err:
        # An expected shape of the file, not a fault to trace.
        LOGGER.error("%s does not name the run: no %s", NODE_RESULTS, err)  # noqa: TRY400
        return NO_VERSION if "tag_no1" in str(err) else NO_NAME
    members = bundle(args.workdir, environment, args.output)
    LOGGER.info("Bundled %s into %s", ", ".join(members), args.output)
    print("\t".join((version, started, environment)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
