#!/usr/bin/env python3
"""Emit a testrun statistics document for upload to the tcache.

Usage:
    sync_tests/scripts/stats_json.py --workdir DIR --junit FILE --exit-code N \
        --kind node|dbsync --environment ENV --output FILE

The counts come from the pytest JUnit file. The sync time, versions and
machine size come from `node_sync_results.json` and `db_sync_results.json`
in the workdir when they exist; a run that failed before writing them is
still reported, timed by the pytest session. The document shape is the one
`PUT /stats` on the tcache takes.
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import math
import os
import pathlib as pl
import sys
import typing as tp
import uuid
import xml.etree.ElementTree as ET

LOGGER = logging.getLogger(__name__)

SCHEMA = 1
STEP = "main"
KINDS = ("node", "dbsync")
DEFAULT_PROJECT = "cardano-sync-tests"
NODE_RESULTS = "node_sync_results.json"
DBSYNC_RESULTS = "db_sync_results.json"

# Worst first, so a case that pytest reports twice gets its worse outcome.
_OUTCOMES = ("failed", "broken", "skipped", "passed")


class Results(tp.NamedTuple):
    """The result files one sync run leaves in its workdir.

    Attributes:
        node: The parsed `node_sync_results.json`, or None when absent.
        dbsync: The parsed `db_sync_results.json`, or None when absent.
    """

    node: dict[str, tp.Any] | None
    dbsync: dict[str, tp.Any] | None


def _utc_now() -> datetime.datetime:
    """Return the current time as a tz-aware UTC datetime."""
    return datetime.datetime.now(tz=datetime.UTC)


def _local_run_id() -> str:
    """Build a sortable run id for a run that has no CI run id."""
    stamp = _utc_now().strftime("%Y%m%dT%H%M%SZ")
    return f"local-{stamp}-{uuid.uuid4().hex[:8]}"


def _project() -> str:
    """Return the project name: `TCACHE_PROJECT`, else the CI repo name, else the default."""
    override = os.environ.get("TCACHE_PROJECT")
    if override:
        return override
    repository = os.environ.get("GITHUB_REPOSITORY")
    if repository:
        return repository.rsplit("/", maxsplit=1)[-1]
    return DEFAULT_PROJECT


def _load_json(path: pl.Path) -> dict[str, tp.Any] | None:
    """Load a JSON object from `path`, or return None when the file is absent.

    Args:
        path: The file to read.

    Returns:
        The parsed object, or None when the file does not exist.
    """
    if not path.is_file():
        return None
    data: dict[str, tp.Any] = json.loads(path.read_text(encoding="utf-8"))
    return data


def read_results(workdir: pl.Path) -> Results:
    """Read whichever result files a sync run left in its workdir.

    Args:
        workdir: The directory the sync run wrote into.

    Returns:
        The node and db-sync results; either may be None.
    """
    return Results(
        node=_load_json(workdir / NODE_RESULTS),
        dbsync=_load_json(workdir / DBSYNC_RESULTS),
    )


def _outcome(case: ET.Element) -> str:
    """Return the outcome of one `<testcase>` element."""
    if case.find("failure") is not None:
        return "failed"
    if case.find("error") is not None:
        return "broken"
    if case.find("skipped") is not None:
        return "skipped"
    return "passed"


def count_junit(path: pl.Path) -> tuple[dict[str, int], float, str | None]:
    """Count the test cases in a pytest JUnit file.

    pytest writes a second element for a case whose teardown errored, so
    cases are grouped by classname and name and the worse outcome wins.

    Args:
        path: The JUnit XML file pytest wrote.

    Returns:
        The five counts, the session duration in seconds, and the session
        start timestamp when the file carries one.

    Raises:
        ValueError: When the file holds no `<testsuite>` element.
    """
    root = ET.parse(path).getroot()
    suite = root if root.tag == "testsuite" else root.find("testsuite")
    if suite is None:
        err = f"{path} holds no <testsuite> element"
        raise ValueError(err)
    outcomes: dict[tuple[str, str], str] = {}
    for case in root.iter("testcase"):
        key = (case.get("classname", ""), case.get("name", ""))
        seen = outcomes.get(key)
        outcome = _outcome(case)
        if seen is None or _OUTCOMES.index(outcome) < _OUTCOMES.index(seen):
            outcomes[key] = outcome
    counts = {"total": len(outcomes)}
    for name in _OUTCOMES:
        counts[name] = sum(1 for value in outcomes.values() if value == name)
    return counts, float(suite.get("time", 0)), suite.get("timestamp")


def _identity(kind: str, env: str) -> dict[str, str]:
    """Build the five fields that identify a run on the tcache.

    Args:
        kind: `node` or `dbsync`.
        env: The network the run synced against.

    Returns:
        project, testrun_name, run_id, step and origin.
    """
    on_ci = bool(os.environ.get("GITHUB_ACTIONS"))
    return {
        "project": _project(),
        "testrun_name": f"sync-{kind}-{env}",
        "run_id": os.environ.get("GITHUB_RUN_ID") or _local_run_id(),
        "step": STEP,
        "origin": "ci" if on_ci else "local",
    }


def _number(value: tp.Any) -> float | None:
    """Return `value` as a float when it is a finite number, else None."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value) if math.isfinite(value) else None


def _sync_seconds(results: Results, kind: str) -> float | None:
    """Return the sync time of the run, in seconds, when the results carry one.

    Args:
        results: The result files of the run.
        kind: `node` or `dbsync`.

    Returns:
        The db-sync sync time for a db-sync run, else the node sync time.
    """
    if kind == "dbsync":
        return _number((results.dbsync or {}).get("total_sync_time_in_sec"))
    return _number((results.node or {}).get("sync_time_seconds1"))


def _started(results: Results, junit_started: str | None) -> str:
    """Return the ISO-8601 time the run started.

    Args:
        results: The result files of the run.
        junit_started: The timestamp from the JUnit file, if any.

    Returns:
        The node sync start, else the db-sync test start, else the JUnit
        timestamp, else now.
    """
    node_start = (results.node or {}).get("start_sync_time1")
    dbsync_start = (results.dbsync or {}).get("start_test_time")
    for candidate in (node_start, dbsync_start, junit_started):
        if isinstance(candidate, str) and candidate:
            return candidate
    return _utc_now().isoformat()


def _versions(results: Results) -> dict[str, str | None]:
    """Collect the software versions the run tested.

    Args:
        results: The result files of the run.

    Returns:
        cardano_node, cardano_cli and cardano_db_sync, None when unknown.
    """
    node = results.node or {}
    dbsync = results.dbsync or {}
    cli = node.get("cli_version1") or dbsync.get("node_cli_version")
    return {
        "cardano_node": node.get("tag_no1") or dbsync.get("node_version"),
        "cardano_cli": cli.split(" - ")[0].removeprefix("cardano-cli ") if cli else None,
        "cardano_db_sync": dbsync.get("db_sync_version"),
    }


def _sync(results: Results, kind: str, env: str) -> dict[str, tp.Any]:
    """Collect the run facts a dashboard shows next to the sync time.

    Args:
        results: The result files of the run.
        kind: `node` or `dbsync`.
        env: The network the run synced against.

    Returns:
        Environment, machine size, chain size and the last synced slot.
    """
    node = results.node or {}
    dbsync = results.dbsync or {}
    source = dbsync or node
    return {
        "env": env,
        "kind": kind,
        "cpu_cores": source.get("no_of_cpu_cores"),
        "ram_gb": source.get("total_ram_in_GB"),
        "platform_release": source.get("platform_release"),
        "chain_size_bytes": node.get("chain_size_bytes"),
        "node_sync_seconds": _number(node.get("sync_time_seconds1")),
        "db_sync_seconds": _number(dbsync.get("total_sync_time_in_sec")),
        "last_slot": dbsync.get("last_synced_slot_no") or node.get("last_slot_no1"),
    }


def build_document(
    workdir: pl.Path, junit: pl.Path, exit_code: int, kind: str, env: str
) -> dict[str, tp.Any]:
    """Build the statistics document for one sync run.

    Args:
        workdir: The directory the sync run wrote into.
        junit: The JUnit XML file pytest wrote.
        exit_code: The pytest exit code.
        kind: `node` or `dbsync`.
        env: The network the run synced against.

    Returns:
        The document to upload.
    """
    results = read_results(workdir)
    counts, session_seconds, junit_started = count_junit(junit)
    duration = _sync_seconds(results, kind)
    return {
        "schema": SCHEMA,
        **_identity(kind, env),
        "timestamp": _started(results, junit_started),
        "duration": session_seconds if duration is None else duration,
        "exit_code": exit_code,
        "counts": counts,
        "quality": {"never_run": 0},
        "session_seconds": session_seconds,
        "versions": _versions(results),
        "sync": _sync(results, kind, env),
    }


def get_args() -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(description="Build the tcache stats document for a sync run.")
    parser.add_argument("-w", "--workdir", required=True, type=pl.Path, help="Sync run workdir.")
    parser.add_argument("-j", "--junit", required=True, type=pl.Path, help="pytest JUnit XML.")
    parser.add_argument("-e", "--exit-code", required=True, type=int, help="pytest exit code.")
    parser.add_argument("-k", "--kind", required=True, choices=KINDS, help="What the run synced.")
    parser.add_argument("-n", "--environment", required=True, help="Network, e.g. mainnet.")
    parser.add_argument("-o", "--output", required=True, type=pl.Path, help="Where to write.")
    return parser.parse_args()


def main() -> int:
    """Write the document, or say why it could not be built.

    Returns:
        0 on success, 1 when an input is missing or unreadable.
    """
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = get_args()
    try:
        document = build_document(
            args.workdir, args.junit, args.exit_code, args.kind, args.environment
        )
    except (OSError, ValueError, ET.ParseError):
        LOGGER.exception("Cannot build the stats document")
        return 1
    args.output.write_text(json.dumps(document, indent=2), encoding="utf-8")
    LOGGER.info(
        "Stats document for %s run %s written to %s",
        document["testrun_name"],
        document["run_id"],
        args.output,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
