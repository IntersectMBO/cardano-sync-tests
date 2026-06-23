"""Tests for mainnet transaction count per epoch updates."""

from __future__ import annotations

import logging

import pytest

from sync_tests.utils import helpers
from sync_tests.utils.external import blockfrost

LOGGER = logging.getLogger(__name__)

pytestmark = pytest.mark.mainnet_tx_count


class TestMainnetTxCount:
    """Fetch and record mainnet transaction counts per epoch."""

    def test_update_tx_count_per_epoch(self, tmp_path):
        """Fetch tx counts for finalized epochs and write to JSON."""
        current_epoch_no = blockfrost.get_current_epoch_no()
        if current_epoch_no is None:
            msg = "Failed to fetch current epoch number from Blockfrost"
            raise RuntimeError(msg)
        LOGGER.info("Current epoch: %d", current_epoch_no)

        # Determine the last epoch we have data for
        last_added_epoch_no = current_epoch_no - 1

        rows = []
        new_epochs_found = False
        for epoch_no in range(last_added_epoch_no, current_epoch_no):
            LOGGER.info("Getting tx count for epoch %d", epoch_no)
            tx_count = blockfrost.get_tx_count_per_epoch(epoch_no)
            LOGGER.info("  tx_count: %s", tx_count)
            rows.append(
                {"epoch_no": epoch_no, "tx_count": tx_count},
            )
            new_epochs_found = True

        if not new_epochs_found:
            helpers.print_message(
                type="warn",
                message="No new finalized epochs to add",
            )
            pytest.skip("No new finalized epochs to process")

        output_file = tmp_path / "mainnet_tx_count_updates.json"
        helpers.write_json_to_file(output_file, rows)
        LOGGER.info("Wrote tx count updates to %s", output_file)

        assert output_file.exists()
        assert len(rows) > 0
