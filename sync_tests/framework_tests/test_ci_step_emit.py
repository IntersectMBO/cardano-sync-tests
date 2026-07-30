"""Regression tests for nix_build.log capture of execute_command output."""

from __future__ import annotations

import os
import pathlib as pl
import subprocess
import sys
import typing as tp

import pytest

from sync_tests.utils import helpers

_REPO_ROOT = pl.Path(__file__).resolve().parents[2]


@pytest.fixture
def nix_build_log(
    tmp_path: pl.Path, monkeypatch: pytest.MonkeyPatch
) -> tp.Generator[pl.Path, None, None]:
    """Enable GitHub Actions logging into a temp nix_build.log."""
    log_path = tmp_path / "nix_build.log"
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("SYNC_TESTS_NIX_BUILD_LOG", str(log_path))
    helpers._nix_build_log_fd = None
    yield log_path
    if helpers._nix_build_log_fd is not None:
        os.close(helpers._nix_build_log_fd)
        helpers._nix_build_log_fd = None


def test_execute_command_appends_to_nix_build_log(nix_build_log: pl.Path) -> None:
    helpers.execute_command("printf 'NIX-BUILD-LINE-1\\nNIX-BUILD-LINE-2\\n'")

    content = nix_build_log.read_text(encoding="utf-8")
    assert "NIX-BUILD-LINE-1" in content
    assert "NIX-BUILD-LINE-2" in content
    assert "Execute command" in content
    assert "Command finished (exit 0)" in content


def test_execute_command_e2e_under_pytest_redirect(tmp_path: pl.Path) -> None:
    """Mimic CI: shell redirect for ci_step.log; nix output must land in nix_build.log."""
    nix_build_log = tmp_path / "nix_build.log"
    ci_step_log = tmp_path / "ci_step.log"
    test_file = tmp_path / "test_exec.py"
    test_file.write_text(
        "from sync_tests.utils.helpers import execute_command\n"
        "def test_run():\n"
        "    execute_command(\"printf 'NIX-E2E-MARKER\\\\n'\")\n"
    )

    env = os.environ.copy()
    env["GITHUB_ACTIONS"] = "true"
    env["SYNC_TESTS_NIX_BUILD_LOG"] = str(nix_build_log)
    env["PYTEST_ADDOPTS"] = "--log-cli-level=WARNING"
    env["PYTHONPATH"] = str(_REPO_ROOT)

    with ci_step_log.open("w") as log_fp:
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", str(test_file)],
            stdout=log_fp,
            stderr=subprocess.STDOUT,
            env=env,
            check=False,
            cwd=_REPO_ROOT,
        )

    assert proc.returncode == 0
    nix_content = nix_build_log.read_text(encoding="utf-8")
    assert "NIX-E2E-MARKER" in nix_content
    assert "Execute command" in nix_content

    ci_content = ci_step_log.read_text(encoding="utf-8")
    assert "NIX-E2E-MARKER" not in ci_content
