"""Tests for sync_tests/scripts/sync_results_zip.py."""

from __future__ import annotations

import json
import pathlib as pl
import zipfile

import pytest

from sync_tests.scripts import sync_results_zip

RESULTS = {
    "env": "preview",
    "tag_no1": "11.2.0",
    "start_sync_time1": "2026-11-12T09:50:44.572113+00:00",
    "sync_time_seconds1": 18984,
}


@pytest.fixture
def workdir(tmp_path: pl.Path) -> pl.Path:
    """Write a workdir as a node sync on preview leaves it, after an earlier mainnet run."""
    (tmp_path / "node_sync_results.json").write_text(json.dumps(RESULTS), encoding="utf-8")
    for name in (
        "sync_markers_preview.json",
        "sync_progress_preview.json",
        "sync_markers_mainnet.json",
    ):
        (tmp_path / name).write_text("{}", encoding="utf-8")
    (tmp_path / "junit.xml").write_text("<testsuites/>", encoding="utf-8")
    (tmp_path / "node_sync.log").write_text("log", encoding="utf-8")
    return tmp_path


def test_the_zip_holds_the_results_and_the_run_network_sync_files(
    workdir: pl.Path, tmp_path: pl.Path
) -> None:
    """The results file first, then the markers and progress of the run's own network."""
    output = tmp_path / "out" / "sync_results.zip"
    output.parent.mkdir()

    members = sync_results_zip.bundle(workdir, "preview", output)

    assert members == [
        "node_sync_results.json",
        "sync_markers_preview.json",
        "sync_progress_preview.json",
    ]
    with zipfile.ZipFile(output) as zf:
        assert zf.namelist() == members
        assert json.loads(zf.read("node_sync_results.json")) == RESULTS


def test_the_run_is_named_by_the_results_file(workdir: pl.Path) -> None:
    """The upload names the run by the version and start time in the results file."""
    assert sync_results_zip.run_name(workdir) == ("11.2.0", "2026-11-12T09:50:44.572113+00:00")


def test_a_run_without_a_version_has_no_name(tmp_path: pl.Path) -> None:
    """A sync of a node given by socket path records no version to file the run under."""
    (tmp_path / "node_sync_results.json").write_text(
        json.dumps(dict(RESULTS, tag_no1=None)), encoding="utf-8"
    )
    assert sync_results_zip.run_name(tmp_path) is None
