"""Tests for sync_tests/scripts/report_stats.sh.

The upload itself is proven against a real tcache. These tests pin the
decisions the script makes on its own: when it stays silent, where it sends
the document, and that an empty 200 from a proxy with no route is not
taken as success.
"""

from __future__ import annotations

import http.server
import json
import pathlib as pl
import subprocess
import threading
import typing as tp

import pytest

SCRIPT = pl.Path(__file__).resolve().parents[1] / "sync_tests" / "scripts" / "report_stats.sh"

JUNIT = (
    '<?xml version="1.0" encoding="utf-8"?><testsuites><testsuite name="pytest" tests="1" '
    'time="1.0" timestamp="2026-10-28T12:38:32+00:00"><testcase classname="t" name="a" '
    'time="1.0" /></testsuite></testsuites>'
)


class _Handler(http.server.BaseHTTPRequestHandler):
    """Answer every PUT with the body the test configured and record the request."""

    body: bytes = b""
    seen: tp.ClassVar[list[tuple[str, dict]]] = []

    def do_PUT(self) -> None:
        """Record the path and document, then answer."""
        length = int(self.headers.get("Content-Length", 0))
        _Handler.seen.append((self.path, json.loads(self.rfile.read(length))))
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, *args: tp.Any) -> None:
        """Keep the test output clean."""


@pytest.fixture
def workdir(tmp_path: pl.Path) -> pl.Path:
    """Write a workdir with one node result and one JUnit file."""
    (tmp_path / "node_sync_results.json").write_text(
        json.dumps({"env": "preview", "tag_no1": "11.1.1"}), encoding="utf-8"
    )
    (tmp_path / "junit.xml").write_text(JUNIT, encoding="utf-8")
    return tmp_path


@pytest.fixture
def server() -> tp.Iterator[str]:
    """Start a local HTTP server and yield its URL, ending in /results like the real secret."""
    _Handler.seen = []
    httpd = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_port}/results"
    finally:
        httpd.shutdown()


def _run(workdir: pl.Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Run the script for a node run on preview with only `env` set."""
    base = {"PATH": "/usr/bin:/bin", "HOME": str(workdir)}
    return subprocess.run(
        ["bash", str(SCRIPT), str(workdir), "0", "node", "preview"],
        env={**base, **env},
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )


def test_silent_when_not_enabled(workdir: pl.Path) -> None:
    """Without the switch the script exits 0 and sends nothing."""
    result = _run(workdir, {"TCACHE_URL": "http://127.0.0.1:9/results", "TCACHE_BASIC_AUTH": "a:b"})

    assert result.returncode == 0
    assert "disabled" in result.stdout


def test_silent_without_secrets(workdir: pl.Path) -> None:
    """With the switch on but no secrets the script exits 0 and sends nothing."""
    result = _run(workdir, {"ENABLE_TCACHE_STATS": "true"})

    assert result.returncode == 0
    assert "disabled" in result.stdout


def test_stores_when_the_service_confirms_the_run(workdir: pl.Path, server: str) -> None:
    """The PUT goes to /stats under the service root and a matching run id is success."""
    _Handler.body = b'{"run_id": "RUN", "project": "cardano-sync-tests"}'
    env = {
        "ENABLE_TCACHE_STATS": "1",
        "TCACHE_URL": server,
        "TCACHE_BASIC_AUTH": "u:p",
        "GITHUB_ACTIONS": "true",
        "GITHUB_RUN_ID": "RUN",
    }

    result = _run(workdir, env)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "stored run RUN" in result.stdout
    assert len(_Handler.seen) == 1
    path, document = _Handler.seen[0]
    assert path == "/stats"
    assert document["testrun_name"] == "sync-node-preview"
    assert document["run_id"] == "RUN"


def test_empty_200_is_a_failure(workdir: pl.Path, server: str) -> None:
    """A 200 with no body, what a proxy with no /stats route returns, is reported as failed."""
    _Handler.body = b""
    env = {
        "ENABLE_TCACHE_STATS": "1",
        "TCACHE_URL": server,
        "TCACHE_BASIC_AUTH": "u:p",
        "GITHUB_ACTIONS": "true",
        "GITHUB_RUN_ID": "RUN",
    }

    result = _run(workdir, env)

    assert result.returncode == 1
    assert "no confirmation" in result.stdout


def test_unreachable_service_names_the_reason(workdir: pl.Path) -> None:
    """When nothing listens, the summary line carries curl's reason, not an empty tail."""
    env = {
        "ENABLE_TCACHE_STATS": "1",
        "TCACHE_URL": "http://127.0.0.1:9/results",
        "TCACHE_BASIC_AUTH": "u:p",
    }

    result = _run(workdir, env)

    assert result.returncode == 1
    assert "failed, upload rejected: curl:" in result.stdout
