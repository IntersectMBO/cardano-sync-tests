"""Project-wide configuration constants derived from environment variables.

Import from this module instead of re-defining constants across utility modules.
All values are resolved once at import time; override via environment variables
before importing to change defaults.

Example:
    Override the base URL before running tests::

        $ CONFIGS_BASE_URL=https://my-mirror.example.com pytest ...
"""

from __future__ import annotations

import os
import typing as tp

#: Base URL for Cardano environment configuration files hosted by IOG.
#: Override via the ``CONFIGS_BASE_URL`` environment variable.
CONFIGS_BASE_URL: tp.Final[str] = (
    os.environ.get("CONFIGS_BASE_URL") or "https://book.world.dev.cardano.org/environments"
)
