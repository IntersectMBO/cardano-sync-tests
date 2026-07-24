"""Tests for sync_tests.scripts.publish_result_summary."""

from __future__ import annotations

import json
import pathlib as pl

import pytest

from sync_tests.scripts import publish_result_summary as prs


def _write_json(path: pl.Path, data: dict) -> None:
    path.write_text(json.dumps(data), encoding="utf-8")


def test_build_summary_lines_no_files_present(tmp_path: pl.Path) -> None:
    assert prs.build_summary_lines(tmp_path) == []


def test_build_summary_lines_node_only(tmp_path: pl.Path) -> None:
    _write_json(
        tmp_path / "node_sync_results.json",
        {
            "env": "preview",
            "tag_no1": "11.0.1",
            "sync_time1": "0:15:32",
            "chain_size_bytes": 2_500_000_000,
            "eras_in_test": ["shelley", "babbage", "conway"],
        },
    )

    lines = prs.build_summary_lines(tmp_path)
    output = "\n".join(lines)

    assert "## Sync results" in output
    assert "### Node sync" in output
    assert "### DB-sync" not in output
    assert "preview" in output
    assert "11.0.1" in output
    assert "0:15:32" in output
    assert "2.3 GB" in output
    assert "shelley, babbage, conway" in output


def test_build_summary_lines_dbsync_only(tmp_path: pl.Path) -> None:
    _write_json(
        tmp_path / "db_sync_results.json",
        {
            "env": "preview",
            "db_sync_revision": "13.7.2.1",
            "total_sync_time_in_h_m_s": "6:23:56",
            "total_database_size": "12 GB",
            "last_synced_epoch_no": 1364,
            "last_synced_block_no": 4488811,
            "rollbacks": False,
            "errors": False,
        },
    )

    lines = prs.build_summary_lines(tmp_path)
    output = "\n".join(lines)

    assert "### DB-sync" in output
    assert "### Node sync" not in output
    assert "13.7.2.1" in output
    assert "6:23:56" in output
    assert "12 GB" in output
    assert "epoch 1364, block 4488811" in output
    assert "Rollbacks detected | No" in output
    assert "Errors detected | No" in output


def test_build_summary_lines_both_present(tmp_path: pl.Path) -> None:
    _write_json(tmp_path / "node_sync_results.json", {"env": "preview"})
    _write_json(tmp_path / "db_sync_results.json", {"env": "preview"})

    lines = prs.build_summary_lines(tmp_path)
    output = "\n".join(lines)

    assert "### Node sync" in output
    assert "### DB-sync" in output


def test_build_summary_lines_rollbacks_and_errors_true(tmp_path: pl.Path) -> None:
    _write_json(
        tmp_path / "db_sync_results.json",
        {"env": "preview", "rollbacks": True, "errors": True},
    )

    output = "\n".join(prs.build_summary_lines(tmp_path))
    assert "Rollbacks detected | Yes" in output
    assert "Errors detected | Yes" in output


def test_build_summary_lines_malformed_json_does_not_raise(tmp_path: pl.Path) -> None:
    (tmp_path / "node_sync_results.json").write_text("not valid json{{{", encoding="utf-8")

    # Must not raise; malformed file is simply skipped.
    assert prs.build_summary_lines(tmp_path) == []


def test_format_bytes() -> None:
    assert prs._format_bytes(500) == "500.0 B"
    assert prs._format_bytes(2_500_000_000) == "2.3 GB"
    assert prs._format_bytes(None) == "unknown"
    assert prs._format_bytes(0) == "unknown"
    assert prs._format_bytes("not-a-number") == "unknown"


def test_publish_writes_to_github_step_summary(
    tmp_path: pl.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    summary_file = tmp_path / "step_summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary_file))

    prs.publish(["## Sync results", "", "### Node sync"])

    assert "### Node sync" in summary_file.read_text(encoding="utf-8")


def test_publish_prints_locally_without_github_step_summary(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)

    prs.publish(["### Node sync"])

    assert "### Node sync" in capsys.readouterr().out


def test_publish_empty_lines_does_nothing(
    tmp_path: pl.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    summary_file = tmp_path / "step_summary.md"
    summary_file.write_text("", encoding="utf-8")
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary_file))

    prs.publish([])

    assert summary_file.read_text(encoding="utf-8") == ""
