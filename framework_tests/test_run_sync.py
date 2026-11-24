"""Tests for sync_tests/scripts/run_sync.sh.

pytest is replaced by a small script that records its arguments and writes
a JUnit file, so the tests pin what the runner adds around it: the report
with the network and run type read from the options, and pytest's exit
code kept whatever the report does.
"""

from __future__ import annotations

import http.server
import json
import pathlib as pl
import stat
import subprocess
import threading
import typing as tp

import pytest

SCRIPT = pl.Path(__file__).resolve().parents[1] / "sync_tests" / "scripts" / "run_sync.sh"

FAKE_PYTEST = """#!/usr/bin/env bash
printf '%s\\n' "$@" > "$FAKE_PYTEST_ARGS"
for ((i = 1; i <= $#; i++)); do
  if [ "${!i}" = "--junitxml" ] && [ -z "${FAKE_PYTEST_NO_JUNIT:-}" ]; then
    j=$((i + 1))
    printf '%s%s%s' '<testsuites><testsuite name="pytest" tests="2" time="3.0">' \
      '<testcase classname="t" name="a" time="1" /><testcase classname="t" name="b" time="2" />' \
      '</testsuite></testsuites>' > "${!j}"
  fi
done
exit "${FAKE_PYTEST_EXIT:-0}"
"""


class _Handler(http.server.BaseHTTPRequestHandler):
    """Store every uploaded document and confirm its run id."""

    seen: tp.ClassVar[list[dict]] = []

    def do_PUT(self) -> None:
        """Record the document and answer with its run id."""
        length = int(self.headers.get("Content-Length", 0))
        doc = json.loads(self.rfile.read(length))
        _Handler.seen.append(doc)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"run_id": doc["run_id"]}).encode())

    def log_message(self, *args: tp.Any) -> None:
        """Keep the test output clean."""


@pytest.fixture
def server() -> tp.Iterator[str]:
    """Start a local tcache stand-in and yield its root URL."""
    _Handler.seen = []
    httpd = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_port}"
    finally:
        httpd.shutdown()


@pytest.fixture
def fake_pytest(tmp_path: pl.Path) -> pl.Path:
    """Put a fake pytest first on PATH and return the file it records its arguments in."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    script = bin_dir / "pytest"
    script.write_text(FAKE_PYTEST, encoding="utf-8")
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return tmp_path / "pytest-args.txt"


def _run(
    tmp_path: pl.Path, args: list[str], env: dict[str, str]
) -> subprocess.CompletedProcess[str]:
    """Run the runner with the fake pytest on PATH and the given extra environment."""
    base = {
        "PATH": f"{tmp_path / 'bin'}:/usr/bin:/bin",
        "HOME": str(tmp_path),
        "FAKE_PYTEST_ARGS": str(tmp_path / "pytest-args.txt"),
        "ENABLE_TCACHE_STATS": "true",
        "TCACHE_BASIC_AUTH": "u:p",
        "GITHUB_ACTIONS": "true",
        "GITHUB_RUN_ID": "RUN",
    }
    return subprocess.run(
        ["bash", str(SCRIPT), *args],
        env={**base, **env},
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
        cwd=tmp_path,
    )


def test_node_run_is_reported_with_its_options(
    tmp_path: pl.Path, fake_pytest: pl.Path, server: str
) -> None:
    """Pytest gets the arguments as given plus the JUnit file; the report names the network."""
    workdir = tmp_path / "wd"
    workdir.mkdir()
    (workdir / "node_sync_results.json").write_text(
        json.dumps({"env": "mainnet", "tag_no1": "11.2.0", "sync_time_seconds1": 132111}),
        encoding="utf-8",
    )
    args = ["sync_tests/tests/x.py", "--environment", "mainnet", "--workdir", str(workdir)]

    result = _run(tmp_path, args, {"TCACHE_URL": server})

    assert result.returncode == 0, result.stdout + result.stderr
    recorded = fake_pytest.read_text(encoding="utf-8").split("\n")
    assert recorded[: len(args)] == args
    assert recorded[len(args) : len(args) + 2] == ["--junitxml", str(workdir / "junit.xml")]
    (doc,) = _Handler.seen
    assert doc["testrun_name"] == "sync-node-mainnet"
    assert doc["duration"] == 132111.0
    assert doc["counts"]["total"] == 2
    assert doc["exit_code"] == 0


def test_dbsync_run_keeps_the_failing_exit_code(
    tmp_path: pl.Path, fake_pytest: pl.Path, server: str
) -> None:
    """A db-sync run is named as one, and a failed pytest still reports and still fails."""
    args = [
        "sync_tests/tests/",
        "--db-sync-revision",
        "13.7.2.1",
        "--workdir",
        str(tmp_path / "wd"),
    ]

    result = _run(tmp_path, args, {"TCACHE_URL": server, "FAKE_PYTEST_EXIT": "1"})

    assert result.returncode == 1
    assert fake_pytest.is_file()
    (doc,) = _Handler.seen
    assert doc["testrun_name"] == "sync-dbsync-preview"
    assert doc["exit_code"] == 1


@pytest.mark.usefixtures("fake_pytest")
def test_an_old_junit_file_is_not_reported(tmp_path: pl.Path, server: str) -> None:
    """When pytest stops before writing a JUnit file, the previous run's counts are not sent."""
    workdir = tmp_path / "wd"
    workdir.mkdir()
    (workdir / "junit.xml").write_text(
        "<testsuites><testsuite tests='9' /></testsuites>", encoding="utf-8"
    )
    args = ["sync_tests/tests/x.py", "--workdir", str(workdir)]

    result = _run(
        tmp_path, args, {"TCACHE_URL": server, "FAKE_PYTEST_EXIT": "4", "FAKE_PYTEST_NO_JUNIT": "1"}
    )

    assert result.returncode == 4
    assert _Handler.seen == []
    assert "failed, no" in result.stdout


def test_a_failed_report_leaves_the_exit_code_alone(
    tmp_path: pl.Path, fake_pytest: pl.Path
) -> None:
    """When nothing listens at the tcache, a green run stays green."""
    args = ["sync_tests/tests/x.py", "--workdir", str(tmp_path / "wd")]

    result = _run(tmp_path, args, {"TCACHE_URL": "http://127.0.0.1:9/results"})

    assert result.returncode == 0
    assert fake_pytest.is_file()
    assert "failed, upload rejected" in result.stdout
