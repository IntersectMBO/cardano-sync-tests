"""Tests for sync_tests.scripts.stats_json."""

from __future__ import annotations

import json
import pathlib as pl

import pytest

from sync_tests.scripts import stats_json

JUNIT = """<?xml version="1.0" encoding="utf-8"?>
<testsuites name="pytest tests">
<testsuite name="pytest" errors="1" failures="1" skipped="1" tests="5" time="12.5"
 timestamp="2026-10-28T12:38:32.000000+00:00" hostname="runner">
<testcase classname="t" name="a" time="1.0" />
<testcase classname="t" name="b" time="1.0" />
<testcase classname="t" name="c" time="0.0"><skipped type="pytest.skip" message="no" /></testcase>
<testcase classname="t" name="d" time="0.0"><failure message="assert 1 == 2">x</failure></testcase>
<testcase classname="t" name="e" time="0.0"><error message="boom">x</error></testcase>
</testsuite>
</testsuites>
"""

JUNIT_TEARDOWN_ERROR = """<?xml version="1.0" encoding="utf-8"?>
<testsuites><testsuite name="pytest" tests="3" time="2.0">
<testcase classname="t" name="a" time="1.0" />
<testcase classname="t" name="b" time="1.0"><failure message="no">x</failure></testcase>
<testcase classname="t" name="b" time="0.0"><error message="teardown">x</error></testcase>
</testsuite></testsuites>
"""

NODE = {
    "env": "mainnet",
    "tag_no1": "11.2.0",
    "cli_version1": "cardano-cli 11.6.0.0 - linux-x86_64 - ghc-9.6",
    "start_sync_time1": "2026-10-28T12:40:00+00:00",
    "sync_time_seconds1": 132111,
    "chain_size_bytes": 239700000000,
    "last_slot_no1": 201746270,
    "no_of_cpu_cores": 32,
    "total_ram_in_GB": 134,
    "platform_release": "6.18.34",
}

DBSYNC = {
    "env": "preview",
    "node_version": "11.1.1",
    "node_cli_version": "cardano-cli 11.2.3.0 - linux-x86_64 - ghc-9.6",
    "db_sync_version": "13.7.2.1",
    "start_test_time": "2026-09-20T10:00:00+00:00",
    "total_sync_time_in_sec": 14852,
    "last_synced_slot_no": 113412361,
    "no_of_cpu_cores": 32,
    "total_ram_in_GB": 134,
    "platform_release": "6.18.34",
}


@pytest.fixture
def junit(tmp_path: pl.Path) -> pl.Path:
    """Write the sample JUnit file and return its path."""
    path = tmp_path / "junit.xml"
    path.write_text(JUNIT, encoding="utf-8")
    return path


@pytest.fixture
def no_ci(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip the CI variables so identity falls back to a local run."""
    for name in ("GITHUB_ACTIONS", "GITHUB_RUN_ID", "GITHUB_REPOSITORY"):
        monkeypatch.delenv(name, raising=False)


def _write(tmp_path: pl.Path, name: str, data: dict) -> None:
    """Write `data` as JSON under `tmp_path`."""
    (tmp_path / name).write_text(json.dumps(data), encoding="utf-8")


def test_counts_each_case_once(junit: pl.Path) -> None:
    """Five cases split into two passed, one failed, one broken, one skipped."""
    counts, seconds, started = stats_json.count_junit(junit)

    assert counts == {"total": 5, "passed": 2, "failed": 1, "broken": 1, "skipped": 1}
    assert seconds == 12.5
    assert started == "2026-10-28T12:38:32.000000+00:00"


def test_a_teardown_error_does_not_count_the_case_twice(tmp_path: pl.Path) -> None:
    """Pytest writes a failed case again for its teardown error; it is one failed case."""
    path = tmp_path / "junit.xml"
    path.write_text(JUNIT_TEARDOWN_ERROR, encoding="utf-8")

    counts, _, _ = stats_json.count_junit(path)

    assert counts == {"total": 2, "passed": 1, "failed": 1, "broken": 0, "skipped": 0}


@pytest.mark.usefixtures("no_ci")
def test_node_run_document(tmp_path: pl.Path, junit: pl.Path) -> None:
    """A node run is named after the network and timed by the node sync."""
    _write(tmp_path, "node_sync_results.json", NODE)

    doc = stats_json.build_document(tmp_path, junit, 0, "node", "mainnet")

    assert doc["schema"] == 1
    assert doc["project"] == "cardano-sync-tests"
    assert doc["testrun_name"] == "sync-node-mainnet"
    assert doc["run_id"].startswith("local-")
    assert doc["step"] == "main"
    assert doc["origin"] == "local"
    assert doc["timestamp"] == "2026-10-28T12:40:00+00:00"
    assert doc["duration"] == 132111.0
    assert doc["exit_code"] == 0
    assert doc["quality"] == {"never_run": 0}
    assert doc["session_seconds"] == 12.5
    assert doc["versions"] == {
        "cardano_node": "11.2.0",
        "cardano_cli": "11.6.0.0",
        "cardano_db_sync": None,
    }
    assert doc["sync"]["kind"] == "node"
    assert doc["sync"]["env"] == "mainnet"
    assert doc["sync"]["cpu_cores"] == 32
    assert doc["sync"]["chain_size_bytes"] == 239700000000
    assert doc["sync"]["last_slot"] == 201746270


@pytest.mark.usefixtures("no_ci")
def test_dbsync_run_document(tmp_path: pl.Path, junit: pl.Path) -> None:
    """A db-sync run is timed by db-sync and carries both sync times."""
    _write(tmp_path, "node_sync_results.json", {**NODE, "env": "preview"})
    _write(tmp_path, "db_sync_results.json", DBSYNC)

    doc = stats_json.build_document(tmp_path, junit, 1, "dbsync", "preview")

    assert doc["testrun_name"] == "sync-dbsync-preview"
    assert doc["duration"] == 14852.0
    assert doc["exit_code"] == 1
    assert doc["versions"]["cardano_db_sync"] == "13.7.2.1"
    assert doc["sync"]["kind"] == "dbsync"
    assert doc["sync"]["node_sync_seconds"] == 132111.0
    assert doc["sync"]["db_sync_seconds"] == 14852.0
    assert doc["sync"]["last_slot"] == 113412361


@pytest.mark.usefixtures("no_ci")
def test_dbsync_run_that_failed_early_keeps_its_name(tmp_path: pl.Path, junit: pl.Path) -> None:
    """A db-sync run that died before writing its results is still a db-sync run."""
    _write(tmp_path, "node_sync_results.json", {**NODE, "env": "preview"})

    doc = stats_json.build_document(tmp_path, junit, 1, "dbsync", "preview")

    assert doc["testrun_name"] == "sync-dbsync-preview"
    assert doc["duration"] == 12.5
    assert doc["sync"]["node_sync_seconds"] == 132111.0
    assert doc["sync"]["db_sync_seconds"] is None


@pytest.mark.usefixtures("no_ci")
def test_run_with_no_results_is_still_reported(tmp_path: pl.Path, junit: pl.Path) -> None:
    """A run whose sync failed has no results file; the session still counts."""
    doc = stats_json.build_document(tmp_path, junit, 1, "node", "mainnet")

    assert doc["testrun_name"] == "sync-node-mainnet"
    assert doc["duration"] == 12.5
    assert doc["timestamp"] == "2026-10-28T12:38:32.000000+00:00"
    assert doc["counts"]["total"] == 5
    assert doc["versions"] == {"cardano_node": None, "cardano_cli": None, "cardano_db_sync": None}


def test_ci_identity_comes_from_the_runner(
    tmp_path: pl.Path, junit: pl.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """On GitHub the run id is the run id and the project is the repo name."""
    _write(tmp_path, "node_sync_results.json", NODE)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_RUN_ID", "40689480314")
    monkeypatch.setenv("GITHUB_REPOSITORY", "IntersectMBO/cardano-sync-tests")

    doc = stats_json.build_document(tmp_path, junit, 0, "node", "mainnet")

    assert doc["origin"] == "ci"
    assert doc["run_id"] == "40689480314"
    assert doc["project"] == "cardano-sync-tests"
