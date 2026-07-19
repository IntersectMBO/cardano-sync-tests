"""Regression test for start_node() building an argv list instead of a split string."""

from __future__ import annotations

import pathlib as pl
import subprocess
import typing as tp

import pytest

from sync_tests.utils import node


def test_start_node_handles_paths_with_spaces(
    tmp_path: pl.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A path containing a space must survive as a single argv element."""
    base_dir = tmp_path / "node base dir"
    conf_dir = tmp_path / "conf dir with spaces"
    socket_path = tmp_path / "socket dir with spaces" / "node.socket"
    monkeypatch.setenv("CARDANO_NODE_SOCKET_PATH", str(socket_path))
    monkeypatch.setattr(node.helpers, "manage_process", lambda *_args, **_kwargs: None)

    captured: dict[str, tp.Any] = {}

    class _FakePopen:
        def __init__(self, cmd: list[str], **_kwargs: tp.Any) -> None:
            captured["cmd"] = cmd

    monkeypatch.setattr(subprocess, "Popen", _FakePopen)

    node.start_node(base_dir=base_dir, node_start_arguments=(), conf_dir=conf_dir)

    cmd = captured["cmd"]
    assert isinstance(cmd, list)
    assert str(conf_dir / "topology.json") in cmd
    assert str(base_dir / "db") in cmd
    assert str(socket_path) in cmd
    assert str(conf_dir / "config.json") in cmd
