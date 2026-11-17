#!/usr/bin/env python3
"""Bundle the results of one node sync run for upload to the tcache.

Usage:
    sync_tests/scripts/sync_results_zip.py --workdir DIR --output FILE --meta FILE

The zip holds `node_sync_results.json` and the sync markers and progress
files next to it, at the zip root like the CI results artifact. The meta
file gets the version and start time the upload names the run by, read
from the results file.
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
EXTRA_PATTERNS = ("sync_markers_*.json", "sync_progress_*.json")


def get_args() -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(description="Bundle node sync results for the tcache.")
    parser.add_argument("--workdir", required=True, type=pl.Path, help="The sync run's workdir.")
    parser.add_argument("--output", required=True, type=pl.Path, help="The zip to write.")
    parser.add_argument("--meta", required=True, type=pl.Path, help="The JSON file to write.")
    return parser.parse_args()


def bundle(workdir: pl.Path, output: pl.Path) -> list[str]:
    """Write the zip and return the names of the files in it.

    Args:
        workdir: The sync run's workdir.
        output: The zip to write.

    Returns:
        The member names, the results file first.
    """
    files = [workdir / NODE_RESULTS]
    for pattern in EXTRA_PATTERNS:
        files.extend(sorted(workdir.glob(pattern)))
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in files:
            zf.write(path, arcname=path.name)
    return [path.name for path in files]


def meta(workdir: pl.Path) -> dict[str, str]:
    """Return the version and start time of the run.

    Args:
        workdir: The sync run's workdir.

    Returns:
        `version` and `started` as the results file records them.

    Raises:
        KeyError: When the results file has no version or start time. A run
            given a socket instead of a node revision records no version.
    """
    doc = json.loads((workdir / NODE_RESULTS).read_text(encoding="utf-8"))
    fields = {"version": doc.get("tag_no1"), "started": doc.get("start_sync_time1")}
    missing = [name for name, value in fields.items() if not value]
    if missing:
        raise KeyError(", ".join(missing))
    return {name: str(value) for name, value in fields.items()}


def main() -> int:
    """Write the zip and the meta file."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = get_args()
    if not (args.workdir / NODE_RESULTS).is_file():
        LOGGER.error("No %s in %s", NODE_RESULTS, args.workdir)
        return 1
    try:
        fields = meta(args.workdir)
    except (ValueError, KeyError):
        LOGGER.exception("%s does not name the run", NODE_RESULTS)
        return 1
    members = bundle(args.workdir, args.output)
    args.meta.write_text(json.dumps(fields), encoding="utf-8")
    LOGGER.info("Bundled %s into %s", ", ".join(members), args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
