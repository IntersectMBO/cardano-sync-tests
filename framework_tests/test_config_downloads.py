"""Regression tests for node config download error handling."""

from __future__ import annotations

import pathlib as pl
import typing as tp

import pytest
import requests

from sync_tests.utils import node


def _http_error(status_code: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    return requests.HTTPError(response=response)


def test_download_config_file_closes_response(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pl.Path
) -> None:
    """The streamed response must be closed, not leaked, after downloading."""
    closed = {"value": False}

    class _FakeResponse:
        def raise_for_status(self) -> None:
            pass

        def iter_content(self, chunk_size: int) -> tp.Iterator[bytes]:  # noqa: ARG002
            yield b"content"

        def __enter__(self) -> _FakeResponse:
            return self

        def __exit__(self, *_args: object) -> None:
            closed["value"] = True

    monkeypatch.setattr(node.helpers, "request_with_retry", lambda *_a, **_kw: _FakeResponse())

    node.download_config_file("preview/config.json", tmp_path / "config.json")

    assert closed["value"] is True


def test_checkpoints_404_is_swallowed(monkeypatch: pytest.MonkeyPatch, tmp_path: pl.Path) -> None:
    """A 404 for checkpoints.json means it genuinely doesn't exist; safe to skip."""

    def _fake_download(config_slug: str, save_as: pl.Path) -> None:
        if "checkpoints.json" in config_slug:
            raise _http_error(404)
        save_as.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(node, "download_config_file", _fake_download)

    # Should not raise.
    node.get_node_config_files(env="preview", node_topology_type="", conf_dir=tmp_path)


def test_checkpoints_server_error_is_not_swallowed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pl.Path
) -> None:
    """A 500 is a real failure and must propagate, not be treated as "not published"."""

    def _fake_download(config_slug: str, save_as: pl.Path) -> None:
        if "checkpoints.json" in config_slug:
            raise _http_error(500)
        save_as.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(node, "download_config_file", _fake_download)

    with pytest.raises(requests.HTTPError):
        node.get_node_config_files(env="preview", node_topology_type="", conf_dir=tmp_path)


def test_peer_snapshot_404_is_swallowed(monkeypatch: pytest.MonkeyPatch, tmp_path: pl.Path) -> None:
    def _fake_download(config_slug: str, save_as: pl.Path) -> None:
        if "peer-snapshot.json" in config_slug:
            raise _http_error(404)
        save_as.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(node, "download_config_file", _fake_download)

    # Should not raise.
    node.get_node_config_files(env="preview", node_topology_type="", conf_dir=tmp_path)


def test_peer_snapshot_server_error_is_not_swallowed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pl.Path
) -> None:
    def _fake_download(config_slug: str, save_as: pl.Path) -> None:
        if "peer-snapshot.json" in config_slug:
            raise _http_error(503)
        save_as.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(node, "download_config_file", _fake_download)

    with pytest.raises(requests.HTTPError):
        node.get_node_config_files(env="preview", node_topology_type="", conf_dir=tmp_path)
