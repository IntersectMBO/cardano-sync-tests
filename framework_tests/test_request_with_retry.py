"""Regression tests for helpers.request_with_retry()."""

from __future__ import annotations

import typing as tp

import pytest
import requests

from sync_tests.utils import helpers


def test_request_with_retry_passes_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every attempt must forward an explicit timeout to requests.request."""
    captured: dict[str, tp.Any] = {}

    def _fake_request(method: str, url: str, **kwargs: tp.Any) -> str:
        captured["method"] = method
        captured["url"] = url
        captured["kwargs"] = kwargs
        return "ok"

    monkeypatch.setattr(requests, "request", _fake_request)

    result = helpers.request_with_retry("get", "https://example.invalid/x")

    assert result == "ok"
    assert captured["kwargs"]["timeout"] == helpers.DEFAULT_HTTP_TIMEOUT_SECONDS


def test_request_with_retry_retries_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient network error must be retried, not raised immediately."""
    monkeypatch.setattr(helpers.time, "sleep", lambda _seconds: None)
    attempts: list[int] = []

    def _flaky_request(method: str, url: str, **kwargs: tp.Any) -> str:  # noqa: ARG001
        attempts.append(1)
        if len(attempts) < 3:
            msg = "boom"
            raise requests.ConnectionError(msg)
        return "ok"

    monkeypatch.setattr(requests, "request", _flaky_request)

    result = helpers.request_with_retry("get", "https://example.invalid/x", max_attempts=3)

    assert result == "ok"
    assert len(attempts) == 3


def test_request_with_retry_raises_after_exhausting_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Once max_attempts is exhausted, the last exception must propagate."""
    monkeypatch.setattr(helpers.time, "sleep", lambda _seconds: None)

    def _always_fails(method: str, url: str, **kwargs: tp.Any) -> str:  # noqa: ARG001
        msg = "still boom"
        raise requests.ConnectionError(msg)

    monkeypatch.setattr(requests, "request", _always_fails)

    with pytest.raises(requests.ConnectionError, match="still boom"):
        helpers.request_with_retry("get", "https://example.invalid/x", max_attempts=2)
