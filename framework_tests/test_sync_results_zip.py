"""Tests for sync_tests/scripts/sync_results_zip.py."""

from __future__ import annotations

import json
import pathlib as pl
import subprocess
import sys
import zipfile

import pytest

from sync_tests.scripts import sync_results_zip

SCRIPT = pl.Path(sync_results_zip.__file__)
RESULTS = {
    "env": "preview",
    "tag_no1": "11.2.0",
    "start_sync_time1": "2026-11-12T09:50:44.572113+00:00",
    "sync_time_seconds1": 18984,
}


@pytest.fixture
def workdir(tmp_path: pl.Path) -> pl.Path:
    """Write a workdir as a node sync on preview leaves it."""
    (tmp_path / "node_sync_results.json").write_text(json.dumps(RESULTS), encoding="utf-8")
    (tmp_path / "sync_markers_preview.json").write_text("{}", encoding="utf-8")
    (tmp_path / "sync_progress_preview.json").write_text("{}", encoding="utf-8")
    (tmp_path / "junit.xml").write_text("<testsuites/>", encoding="utf-8")
    (tmp_path / "node_sync.log").write_text("log", encoding="utf-8")
    return tmp_path


def test_the_zip_holds_the_results_and_the_sync_files(workdir: pl.Path, tmp_path: pl.Path) -> None:
    """The results file first, then the markers and progress; logs and JUnit stay out."""
    output = tmp_path / "out" / "sync_results.zip"
    output.parent.mkdir()

    members = sync_results_zip.bundle(workdir, output)

    assert members == [
        "node_sync_results.json",
        "sync_markers_preview.json",
        "sync_progress_preview.json",
    ]
    with zipfile.ZipFile(output) as zf:
        assert zf.namelist() == members
        assert json.loads(zf.read("node_sync_results.json")) == RESULTS


def test_the_meta_names_the_run(workdir: pl.Path) -> None:
    assert sync_results_zip.meta(workdir) == {
        "version": "11.2.0",
        "started": "2026-11-12T09:50:44.572113+00:00",
    }


def test_the_script_writes_both_files(workdir: pl.Path, tmp_path: pl.Path) -> None:
    output = tmp_path / "sync_results.zip"
    meta = tmp_path / "meta.json"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--workdir",
            str(workdir),
            "--output",
            str(output),
            "--meta",
            str(meta),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert zipfile.is_zipfile(output)
    assert json.loads(meta.read_text(encoding="utf-8"))["version"] == "11.2.0"


def test_a_run_without_a_version_is_refused(tmp_path: pl.Path) -> None:
    """A sync of a node given by socket path records no version to file the run under."""
    (tmp_path / "node_sync_results.json").write_text(
        json.dumps(dict(RESULTS, tag_no1=None)), encoding="utf-8"
    )
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--workdir",
            str(tmp_path),
            "--output",
            str(tmp_path / "sync_results.zip"),
            "--meta",
            str(tmp_path / "meta.json"),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "does not name the run" in result.stderr
    assert not (tmp_path / "sync_results.zip").exists()


def test_a_run_that_wrote_no_results_is_refused(tmp_path: pl.Path) -> None:
    """A sync that failed before writing results has nothing the analyzer can use."""
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--workdir",
            str(tmp_path),
            "--output",
            str(tmp_path / "sync_results.zip"),
            "--meta",
            str(tmp_path / "meta.json"),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "No node_sync_results.json" in result.stderr
    assert not (tmp_path / "sync_results.zip").exists()
