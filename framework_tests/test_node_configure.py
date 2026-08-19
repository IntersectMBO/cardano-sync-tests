"""Tests for patching the node config for resource monitoring."""

from __future__ import annotations

import json
import pathlib as pl
import typing as tp

import pytest

from sync_tests.utils import node

# The relevant part of the config as distributed by IOG.
IOG_TRACE_OPTIONS: tp.Final = {
    "": {
        "backends": [
            "EKGBackend",
            "Forwarder",
            "PrometheusSimple suffix 127.0.0.1 12798",
            "Stdout HumanFormatColoured",
        ],
        "detail": "DNormal",
        "severity": "Notice",
    },
    "ChainDB": {"severity": "Info"},
    "Resources": {"severity": "Silence"},
}


def _configure(config: dict, tmp_path: pl.Path, cli_version: str | None = None) -> dict:
    config_file = tmp_path / "config.json"
    with open(config_file, "w") as fh:
        json.dump(config, fh)
    node.configure_node(config_file=config_file, cli_version=cli_version)
    with open(config_file) as fh:
        patched: dict = json.load(fh)
    return patched


def test_configure_node_unsilences_resources(tmp_path: pl.Path) -> None:
    """Resource stats must reach the node log, on the only backend we read."""
    patched = _configure({"TraceOptions": IOG_TRACE_OPTIONS}, tmp_path=tmp_path)
    trace_opts = patched["TraceOptions"]

    assert trace_opts["Resources"]["severity"] == "Info"
    assert trace_opts[""]["backends"] == ["Stdout HumanFormatUncoloured"]
    # Severity of the other tracers is left as configured.
    assert trace_opts[""]["severity"] == "Notice"
    assert trace_opts["ChainDB"] == {"severity": "Info"}


def test_configure_node_selects_new_tracing_without_switch(tmp_path: pl.Path) -> None:
    """A config with `TraceOptions` and no switch is new tracing; make that explicit."""
    patched = _configure({"TraceOptions": IOG_TRACE_OPTIONS}, tmp_path=tmp_path)

    assert patched["UseTraceDispatcher"] is True
    assert "options" not in patched


def test_configure_node_respects_explicit_new_tracing_switch(tmp_path: pl.Path) -> None:
    """`UseTraceDispatcher: true` selects new tracing even without `TraceOptions`."""
    patched = _configure({"UseTraceDispatcher": True}, tmp_path=tmp_path)

    assert patched["UseTraceDispatcher"] is True
    assert patched["TraceOptions"]["Resources"]["severity"] == "Info"
    assert patched["TraceOptions"][""]["backends"] == ["Stdout HumanFormatUncoloured"]
    assert "options" not in patched


def test_configure_node_treats_null_switch_as_unset(tmp_path: pl.Path) -> None:
    """A `null` switch is what the node sees as unset, not as legacy."""
    patched = _configure(
        {"UseTraceDispatcher": None, "TraceOptions": IOG_TRACE_OPTIONS}, tmp_path=tmp_path
    )

    assert patched["UseTraceDispatcher"] is True
    assert "options" not in patched


def test_configure_node_treats_null_switch_without_trace_options_as_legacy(
    tmp_path: pl.Path,
) -> None:
    """An unset switch on a config without `TraceOptions` still means legacy."""
    patched = _configure({"UseTraceDispatcher": None}, tmp_path=tmp_path)

    assert patched["UseTraceDispatcher"] is False
    assert patched["options"]["mapBackends"]["cardano.node.resources"] == ["KatipBK"]


def test_configure_node_ignores_non_boolean_switch(tmp_path: pl.Path) -> None:
    """A value the node cannot use is replaced by the detected one, not interpreted."""
    patched = _configure(
        {"UseTraceDispatcher": "false", "TraceOptions": IOG_TRACE_OPTIONS}, tmp_path=tmp_path
    )

    assert patched["UseTraceDispatcher"] is True
    assert "options" not in patched


def test_configure_node_ignores_non_boolean_switch_without_trace_options(
    tmp_path: pl.Path,
) -> None:
    """Detection falls back to `TraceOptions`, which a legacy config does not have."""
    patched = _configure({"UseTraceDispatcher": "false"}, tmp_path=tmp_path)

    assert patched["UseTraceDispatcher"] is False
    assert patched["options"]["mapBackends"]["cardano.node.resources"] == ["KatipBK"]


def test_configure_node_respects_explicit_legacy_switch(tmp_path: pl.Path) -> None:
    """`UseTraceDispatcher: false` means legacy tracing, whatever else is in the config."""
    patched = _configure(
        {"UseTraceDispatcher": False, "TraceOptions": IOG_TRACE_OPTIONS}, tmp_path=tmp_path
    )

    assert patched["UseTraceDispatcher"] is False
    assert patched["options"]["mapBackends"]["cardano.node.resources"] == ["KatipBK"]
    # New tracing is configured as well, for nodes that no longer know the switch.
    assert patched["TraceOptions"]["Resources"]["severity"] == "Info"


def test_configure_node_configures_legacy_style_config(tmp_path: pl.Path) -> None:
    """A config without `TraceOptions` predates new tracing."""
    patched = _configure({"minSeverity": "Critical"}, tmp_path=tmp_path)

    assert patched["UseTraceDispatcher"] is False
    assert patched["minSeverity"] == "Info"
    assert patched["options"]["mapBackends"]["cardano.node.resources"] == ["KatipBK"]
    assert patched["TraceOptions"]["Resources"]["severity"] == "Info"


def test_configure_node_is_idempotent(tmp_path: pl.Path) -> None:
    """Configuring an already configured config must not change it."""
    configs = (
        {"TraceOptions": IOG_TRACE_OPTIONS},
        {"minSeverity": "Critical"},
        {"UseTraceDispatcher": True},
        {"UseTraceDispatcher": None, "TraceOptions": IOG_TRACE_OPTIONS},
        {"UseTraceDispatcher": None},
        {"UseTraceDispatcher": "false", "TraceOptions": IOG_TRACE_OPTIONS},
    )
    for config in configs:
        patched = _configure(config, tmp_path=tmp_path)
        assert _configure(patched, tmp_path=tmp_path) == patched


def _write_peer_snapshot(tmp_path: pl.Path) -> pl.Path:
    """Write a version 2 peer snapshot and the topology referencing it."""
    topology_file = tmp_path / "topology.json"
    with open(topology_file, "w") as fh:
        json.dump({"peerSnapshotFile": "peer-snapshot.json"}, fh)

    snapshot_file = tmp_path / "peer-snapshot.json"
    with open(snapshot_file, "w") as fh:
        json.dump(
            {
                "version": 2,
                "bigLedgerPools": [{"relays": [{"address": "1.2.3.4", "port": 3001}]}],
            },
            fh,
        )
    return snapshot_file


@pytest.mark.parametrize(
    ("cli_version", "expected_version"),
    [
        # Nodes older than 10.5 cannot parse a version 2 peer snapshot.
        ("cli 10.4.0", 1),
        # From 10.5 on the node parses a version 2 peer snapshot itself.
        ("cli 10.5.0", 2),
        # Without a version to go by, downgrade rather than risk an unreadable snapshot.
        (None, 1),
        # An unparsable version is as good as no version.
        ("cli unknown", 1),
    ],
)
def test_configure_node_peer_snapshot_version(
    tmp_path: pl.Path, cli_version: str | None, expected_version: int
) -> None:
    """The peer snapshot is downgraded only for nodes that need it."""
    snapshot_file = _write_peer_snapshot(tmp_path=tmp_path)
    _configure({"TraceOptions": IOG_TRACE_OPTIONS}, tmp_path=tmp_path, cli_version=cli_version)

    with open(snapshot_file) as fh:
        snapshot = json.load(fh)
    assert snapshot["version"] == expected_version
    assert snapshot["bigLedgerPools"][0]["relays"]


def test_configure_node_resolves_relay_domains_on_downgrade(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pl.Path
) -> None:
    """Version 1 accepts IP addresses only, so domain relays are resolved, not dropped."""
    snapshot_file = _write_peer_snapshot(tmp_path=tmp_path)
    with open(snapshot_file) as fh:
        snapshot = json.load(fh)
    snapshot["bigLedgerPools"][0]["relays"].append({"address": "relay.example.com", "port": 3001})
    with open(snapshot_file, "w") as fh:
        json.dump(snapshot, fh)

    monkeypatch.setattr(node.socket, "gethostbyname", lambda _: "5.6.7.8")
    _configure({"TraceOptions": IOG_TRACE_OPTIONS}, tmp_path=tmp_path, cli_version="cli 10.4.0")

    with open(snapshot_file) as fh:
        downgraded = json.load(fh)
    assert downgraded["version"] == 1
    relays = downgraded["bigLedgerPools"][0]["relays"]
    assert [r["address"] for r in relays] == ["1.2.3.4", "5.6.7.8"]
