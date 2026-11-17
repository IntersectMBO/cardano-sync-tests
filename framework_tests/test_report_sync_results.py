"""Tests for sync_tests/scripts/report_sync_results.sh.

The upload itself is proven against a real tcache. These tests pin the
decisions the script makes on its own: when it stays silent, what it sends
and where, and that an empty 200 from a proxy with no route is not taken as
success.
"""

from __future__ import annotations

import email.parser
import email.policy
import http.server
import io
import json
import pathlib as pl
import subprocess
import threading
import typing as tp
import zipfile

import pytest

SCRIPT = (
    pl.Path(__file__).resolve().parents[1] / "sync_tests" / "scripts" / "report_sync_results.sh"
)
RESULTS = {"env": "preview", "tag_no1": "11.2.0", "start_sync_time1": "2026-11-12T09:50:44+00:00"}


class _Handler(http.server.BaseHTTPRequestHandler):
    """Answer every PUT with the body the test configured and record the request."""

    body: bytes = b""
    seen: tp.ClassVar[list[tuple[str, dict]]] = []

    def do_PUT(self) -> None:
        """Record the path and the multipart fields, then answer."""
        length = int(self.headers.get("Content-Length", 0))
        raw = f"Content-Type: {self.headers['Content-Type']}\r\n\r\n".encode() + self.rfile.read(
            length
        )
        message = email.parser.BytesParser(policy=email.policy.HTTP).parsebytes(raw)
        fields = {}
        for part in message.iter_parts():
            payload = part.get_payload(decode=True)
            name = part.get_param("name", header="content-disposition")
            fields[name] = payload if part.get_filename() else payload.decode()
            if part.get_filename():
                fields["filename"] = part.get_filename()
        _Handler.seen.append((self.path, fields))
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, *args: tp.Any) -> None:
        """Keep the test output clean."""


@pytest.fixture
def workdir(tmp_path: pl.Path) -> pl.Path:
    """Write a workdir with one node result."""
    (tmp_path / "node_sync_results.json").write_text(json.dumps(RESULTS), encoding="utf-8")
    (tmp_path / "sync_markers_preview.json").write_text("{}", encoding="utf-8")
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
    """Run the script for a run on preview with only `env` set."""
    base = {"PATH": "/usr/bin:/bin", "HOME": str(workdir)}
    return subprocess.run(
        ["bash", str(SCRIPT), str(workdir), "preview"],
        env={**base, **env},
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )


def test_silent_when_not_enabled(workdir: pl.Path) -> None:
    """Without the switch the script exits 0 and sends nothing."""
    result = _run(workdir, {"TCACHE_URL": "http://127.0.0.1:9/results", "TCACHE_BASIC_AUTH": "a:b"})

    assert result.returncode == 0
    assert "disabled" in result.stdout


def test_stores_when_the_service_confirms_the_run(workdir: pl.Path, server: str) -> None:
    """The PUT goes to /sync-results/<version> under the service root with the run's fields."""
    _Handler.body = b'{"sync_results": "11.2.0", "id": 7}'
    env = {"ENABLE_TCACHE_STATS": "1", "TCACHE_URL": server, "TCACHE_BASIC_AUTH": "u:p"}

    result = _run(workdir, env)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "stored 11.2.0 on preview as run 7" in result.stdout
    (request,) = _Handler.seen
    path, fields = request
    assert path == "/sync-results/11.2.0"
    assert fields["environment"] == "preview"
    assert fields["started"] == "2026-11-12T09:50:44+00:00"
    # The service accepts a `.zip` file name only.
    assert fields["filename"] == "sync_results.zip"
    with zipfile.ZipFile(io.BytesIO(fields["syncresults"])) as zf:
        assert zf.namelist() == ["node_sync_results.json", "sync_markers_preview.json"]


def test_a_branch_name_is_made_safe_for_the_path(tmp_path: pl.Path, server: str) -> None:
    """A run of a branch records the branch name as its version; the path cannot hold a slash."""
    (tmp_path / "node_sync_results.json").write_text(
        json.dumps(dict(RESULTS, tag_no1="feat/faster-sync")), encoding="utf-8"
    )
    _Handler.body = b'{"sync_results": "feat-faster-sync", "id": 2}'
    env = {"ENABLE_TCACHE_STATS": "1", "TCACHE_URL": server, "TCACHE_BASIC_AUTH": "u:p"}

    result = _run(tmp_path, env)

    assert result.returncode == 0, result.stdout + result.stderr
    assert _Handler.seen[0][0] == "/sync-results/feat-faster-sync"


def test_an_answer_for_another_run_is_a_failure(workdir: pl.Path, server: str) -> None:
    """A JSON 200 that names some other run, as a mislaid proxy route could, is not success."""
    _Handler.body = b'{"sync_results": "11.1.0", "id": 3}'
    env = {"ENABLE_TCACHE_STATS": "1", "TCACHE_URL": server, "TCACHE_BASIC_AUTH": "u:p"}

    result = _run(workdir, env)

    assert result.returncode == 1
    assert "no confirmation" in result.stdout


def test_empty_200_is_a_failure(workdir: pl.Path, server: str) -> None:
    """A 200 with no body, what a proxy with no route returns, is reported as failed."""
    _Handler.body = b""
    env = {"ENABLE_TCACHE_STATS": "1", "TCACHE_URL": server, "TCACHE_BASIC_AUTH": "u:p"}

    result = _run(workdir, env)

    assert result.returncode == 1
    assert "no confirmation" in result.stdout


def test_a_run_without_results_is_a_failure(tmp_path: pl.Path, server: str) -> None:
    """A sync that wrote no results file sends nothing and says so."""
    env = {"ENABLE_TCACHE_STATS": "1", "TCACHE_URL": server, "TCACHE_BASIC_AUTH": "u:p"}

    result = _run(tmp_path, env)

    assert result.returncode == 1
    assert "failed, no" in result.stdout
    assert _Handler.seen == []


def test_a_run_without_a_version_is_skipped(tmp_path: pl.Path, server: str) -> None:
    """A sync of a node given by socket path has no version to file the run under."""
    (tmp_path / "node_sync_results.json").write_text(
        json.dumps(dict(RESULTS, tag_no1=None)), encoding="utf-8"
    )
    env = {"ENABLE_TCACHE_STATS": "1", "TCACHE_URL": server, "TCACHE_BASIC_AUTH": "u:p"}

    result = _run(tmp_path, env)

    assert result.returncode == 0
    assert "skipped, the results name no node version" in result.stdout
    assert _Handler.seen == []
