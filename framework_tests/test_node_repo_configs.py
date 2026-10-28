"""Tests for taking the node config files from the cardano-node repository."""

from __future__ import annotations

import json
import pathlib as pl

import pytest

from sync_tests.utils import node

ENV = "mainnet"

# The shape of the files a node 11.2 revision ships: a cardano-config envelope
# that references its siblings by their prefixed names.
SHIPPED_CONFIG = {
    "$schema": "https://example.invalid/config.schema.json",
    "Version": 2,
    "MinNodeVersion": "11.2.0",
    "Configuration": {
        "Consensus": {"ConsensusMode": "GenesisMode"},
        "Protocol": {
            "ByronGenesisFile": "mainnet-byron-genesis.json",
            "ShelleyGenesisFile": "mainnet-shelley-genesis.json",
            "AlonzoGenesisFile": "mainnet-alonzo-genesis.json",
            "ConwayGenesisFile": "mainnet-conway-genesis.json",
            "CheckpointsFile": "mainnet-checkpoints.json",
            "CheckpointsFileHash": "mainnet-is-not-a-file-name",
        },
    },
}
SHIPPED_TOPOLOGY = {
    "bootstrapPeers": [{"address": "backbone.example.com", "port": 3001}],
    "peerSnapshotFile": "mainnet-peer-snapshot.json",
    "useLedgerAfterSlot": 1,
}
SHIPPED_PEER_SNAPSHOT = {
    "bigLedgerPools": [{"relays": [{"domain": "relay.example.com", "port": 3001}]}]
}


def _write_node_repo(repo_dir: pl.Path, with_optional: bool = True) -> pl.Path:
    """Lay out ``configuration/cardano`` the way the cardano-node repository does."""
    cfg_dir = repo_dir / node.NODE_REPO_CONFIG_DIR
    cfg_dir.mkdir(parents=True)
    files: dict[str, object] = {
        "config.json": SHIPPED_CONFIG,
        "topology.json": SHIPPED_TOPOLOGY,
        "byron-genesis.json": {"byron": True},
        "shelley-genesis.json": {"shelley": True},
        "alonzo-genesis.json": {"alonzo": True},
        "conway-genesis.json": {"conway": True},
    }
    if with_optional:
        files["checkpoints.json"] = {"checkpoints": []}
        files["peer-snapshot.json"] = SHIPPED_PEER_SNAPSHOT
    for name, content in files.items():
        with open(cfg_dir / f"{ENV}-{name}", "w") as fh:
            json.dump(content, fh)
    return cfg_dir


def _read(path: pl.Path) -> dict:
    with open(path) as fh:
        data: dict = json.load(fh)
    return data


def test_node_repo_configs_are_copied_under_expected_names(tmp_path: pl.Path) -> None:
    """Every shipped file lands in ``conf_dir`` without the environment prefix."""
    repo_dir = tmp_path / "repo"
    conf_dir = tmp_path / "conf"
    conf_dir.mkdir()
    _write_node_repo(repo_dir)

    node.get_node_repo_config_files(env=ENV, node_repo_dir=repo_dir, conf_dir=conf_dir)

    assert sorted(p.name for p in conf_dir.iterdir()) == [
        "alonzo-genesis.json",
        "byron-genesis.json",
        "checkpoints.json",
        "config.json",
        "conway-genesis.json",
        "peer-snapshot.json",
        "shelley-genesis.json",
        "topology.json",
    ]
    assert _read(conf_dir / "shelley-genesis.json") == {"shelley": True}


def test_node_repo_config_references_follow_the_rename(tmp_path: pl.Path) -> None:
    """The config and topology point at the copied names, nested keys included."""
    repo_dir = tmp_path / "repo"
    conf_dir = tmp_path / "conf"
    conf_dir.mkdir()
    _write_node_repo(repo_dir)

    node.get_node_repo_config_files(env=ENV, node_repo_dir=repo_dir, conf_dir=conf_dir)

    protocol = _read(conf_dir / "config.json")["Configuration"]["Protocol"]
    assert protocol["ByronGenesisFile"] == "byron-genesis.json"
    assert protocol["ShelleyGenesisFile"] == "shelley-genesis.json"
    assert protocol["AlonzoGenesisFile"] == "alonzo-genesis.json"
    assert protocol["ConwayGenesisFile"] == "conway-genesis.json"
    assert protocol["CheckpointsFile"] == "checkpoints.json"
    # Only file names are renamed; a value that merely starts with the prefix is not.
    assert protocol["CheckpointsFileHash"] == "mainnet-is-not-a-file-name"
    assert _read(conf_dir / "topology.json")["peerSnapshotFile"] == "peer-snapshot.json"


def test_node_repo_config_keeps_the_shipped_format(tmp_path: pl.Path) -> None:
    """The envelope is copied as is; nothing is flattened or added."""
    repo_dir = tmp_path / "repo"
    conf_dir = tmp_path / "conf"
    conf_dir.mkdir()
    _write_node_repo(repo_dir)

    node.get_node_repo_config_files(env=ENV, node_repo_dir=repo_dir, conf_dir=conf_dir)

    config = _read(conf_dir / "config.json")
    assert sorted(config) == ["$schema", "Configuration", "MinNodeVersion", "Version"]
    assert config["Configuration"]["Consensus"] == {"ConsensusMode": "GenesisMode"}


def test_node_repo_peer_snapshot_is_normalized(tmp_path: pl.Path) -> None:
    """The copied peer snapshot gets the same domain-to-address fix as a download."""
    repo_dir = tmp_path / "repo"
    conf_dir = tmp_path / "conf"
    conf_dir.mkdir()
    _write_node_repo(repo_dir)

    node.get_node_repo_config_files(env=ENV, node_repo_dir=repo_dir, conf_dir=conf_dir)

    relay = _read(conf_dir / "peer-snapshot.json")["bigLedgerPools"][0]["relays"][0]
    assert relay == {"address": "relay.example.com", "port": 3001}


def test_node_repo_optional_files_may_be_missing(tmp_path: pl.Path) -> None:
    """A revision without checkpoints or a peer snapshot still provides a config."""
    repo_dir = tmp_path / "repo"
    conf_dir = tmp_path / "conf"
    conf_dir.mkdir()
    _write_node_repo(repo_dir, with_optional=False)

    node.get_node_repo_config_files(env=ENV, node_repo_dir=repo_dir, conf_dir=conf_dir)

    assert not (conf_dir / "checkpoints.json").exists()
    assert not (conf_dir / "peer-snapshot.json").exists()
    assert (conf_dir / "config.json").is_file()


def test_node_repo_without_configs_for_env_fails(tmp_path: pl.Path) -> None:
    """A revision that ships no files for the environment is an error, not a fallback."""
    repo_dir = tmp_path / "repo"
    conf_dir = tmp_path / "conf"
    conf_dir.mkdir()
    _write_node_repo(repo_dir)

    with pytest.raises(FileNotFoundError, match="preview-config.json"):
        node.get_node_repo_config_files(env="preview", node_repo_dir=repo_dir, conf_dir=conf_dir)
