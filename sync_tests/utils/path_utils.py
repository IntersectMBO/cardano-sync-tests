"""Shared path helpers for repository-relative directories."""

from __future__ import annotations

import pathlib as pl


def get_repo_root() -> pl.Path:
    """Get the root directory of the repository."""
    return pl.Path(__file__).parent.parent.parent


def get_db_sync_dir() -> pl.Path:
    """Get the cardano-db-sync directory path."""
    return get_repo_root() / "cardano-db-sync"
