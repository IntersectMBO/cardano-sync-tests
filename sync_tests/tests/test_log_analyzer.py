"""Tests for sync_tests.utils.logs.log_analyzer."""

from __future__ import annotations

import logging
import pathlib as pl

import pytest

from sync_tests.utils.logs import log_analyzer


def _write(tmp_path: pl.Path, name: str, content: str) -> pl.Path:
    log_file = tmp_path / name
    log_file.write_text(content, encoding="utf-8")
    return log_file


def test_is_string_present_in_file_finds_match(tmp_path: pl.Path) -> None:
    log_file = _write(tmp_path, "a.log", "line one\nline two: db-sync-node:Error boom\n")
    assert log_analyzer.is_string_present_in_file(log_file, "db-sync-node:Error") is True


def test_is_string_present_in_file_no_match(tmp_path: pl.Path) -> None:
    log_file = _write(tmp_path, "a.log", "line one\nline two\n")
    assert log_analyzer.is_string_present_in_file(log_file, "db-sync-node:Error") is False


def test_is_string_present_in_file_treats_search_string_literally(tmp_path: pl.Path) -> None:
    """Regex metacharacters in the search string must not be interpreted as a pattern."""
    log_file = _write(tmp_path, "a.log", "cost 3.14 not 3x14\n")
    # "3.14" as a regex would also match "3x14"; as a literal string it must not.
    assert log_analyzer.is_string_present_in_file(log_file, "3.14") is True
    no_match_file = _write(tmp_path, "b.log", "3x14\n")
    assert log_analyzer.is_string_present_in_file(no_match_file, "3.14") is False


def test_are_rollbacks_present_no_occurrences(tmp_path: pl.Path) -> None:
    log_file = _write(tmp_path, "a.log", "nothing interesting here\n")
    assert log_analyzer.are_rollbacks_present_in_logs(log_file) is False


def test_are_rollbacks_present_single_occurrence(tmp_path: pl.Path) -> None:
    """Documents current behavior: a single "rolling" mention is not reported as a rollback.

    are_rollbacks_present_in_logs() only returns True once "rolling" is found a second
    time, so exactly one rollback-looking line in the log is treated the same as zero.
    """
    log_file = _write(tmp_path, "a.log", "rolling back to slot 123\nall good after that\n")
    assert log_analyzer.are_rollbacks_present_in_logs(log_file) is False


def test_are_rollbacks_present_two_occurrences(tmp_path: pl.Path) -> None:
    log_file = _write(
        tmp_path,
        "a.log",
        "rolling back to slot 123\nsome other line\nrolling back to slot 456\n",
    )
    assert log_analyzer.are_rollbacks_present_in_logs(log_file) is True


def test_check_db_sync_logs_warns_on_each_condition(
    tmp_path: pl.Path, caplog: pytest.LogCaptureFixture
) -> None:
    log_file = _write(
        tmp_path,
        "db_sync.log",
        "db-sync-node:Error something broke\n"
        "Rollback failed badly\n"
        "Failed to parse ledger state\n"
        "rolling back to slot 1\n"
        "rolling back to slot 2\n",
    )
    with caplog.at_level(logging.WARNING):
        log_analyzer.check_db_sync_logs(log_file=log_file)

    messages = " ".join(caplog.messages)
    assert "Errors present" in messages
    assert "Rollbacks present" in messages
    assert "Failed rollbacks present" in messages
    assert "Corrupted ledger files present" in messages


def test_check_db_sync_logs_silent_on_clean_log(
    tmp_path: pl.Path, caplog: pytest.LogCaptureFixture
) -> None:
    log_file = _write(tmp_path, "db_sync.log", "Inserted 5 EpochStake for EpochNo 10\n")
    with caplog.at_level(logging.WARNING):
        log_analyzer.check_db_sync_logs(log_file=log_file)

    assert caplog.messages == []
