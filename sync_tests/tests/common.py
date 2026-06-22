"""Shared pytest skip markers and helpers reused across test modules.

Import the ``SKIPIF_*`` constants directly instead of duplicating
``pytest.mark.skipif`` expressions in every test file.

Example:
    Apply a marker to a single test::

        from sync_tests.tests.common import SKIPIF_NO_NODE_REVISION

        @SKIPIF_NO_NODE_REVISION
        def test_something(...):
            ...
"""

from __future__ import annotations

import os

import pytest

#: Skip when ``--node-revision`` / ``NODE_REVISION`` env-var is absent.
SKIPIF_NO_NODE_REVISION = pytest.mark.skipif(
    not os.environ.get("NODE_REVISION"),
    reason="NODE_REVISION env-var not set; pass --node-revision or export NODE_REVISION",
)

#: Skip when ``--db-sync-revision`` / ``DB_SYNC_REVISION`` env-var is absent.
SKIPIF_NO_DB_SYNC_REVISION = pytest.mark.skipif(
    not os.environ.get("DB_SYNC_REVISION"),
    reason="DB_SYNC_REVISION env-var not set; pass --db-sync-revision or export DB_SYNC_REVISION",
)

#: Skip when ``CARDANO_NODE_SOCKET_PATH`` is not set or the socket file does not exist.
SKIPIF_NO_NODE_SOCKET = pytest.mark.skipif(
    not os.environ.get("CARDANO_NODE_SOCKET_PATH"),
    reason="CARDANO_NODE_SOCKET_PATH not set; node must be running before this test",
)
