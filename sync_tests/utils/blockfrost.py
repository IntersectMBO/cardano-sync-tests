import logging
import os
from datetime import datetime

from blockfrost import BlockFrostApi

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_blockfrost_api():
    """Initialize and return the BlockFrost API client."""
    project_id = os.environ.get("BLOCKFROST_API_KEY")
    if not project_id:
        raise ValueError("BLOCKFROST_API_KEY is not set.")
    return BlockFrostApi(project_id=project_id)


def is_blockfrost_healthy(api):
    """Check if the Blockfrost API is healthy."""
    try:
        health = api.health(return_type="json")
        return health.get("is_healthy", False)
    except Exception as e:
        logging.exception(f"Error checking Blockfrost health: {e}")
        return False


def get_tx_count_per_epoch(epoch_no):
    """Get the transaction count for a given epoch."""
    logging.info(f"Fetching transaction count for epoch {epoch_no}")
    api = get_blockfrost_api()
    if is_blockfrost_healthy(api):
        try:
            return api.epoch(number=epoch_no).tx_count
        except Exception as e:
            logging.exception(
                f"Error fetching transaction count for epoch {epoch_no}: {e}"
            )
            return None
    else:
        logging.error("Blockfrost API is unhealthy.")
        return None


def get_current_epoch_no():
    """Get the current epoch number."""
    logging.info("Fetching current epoch number")
    api = get_blockfrost_api()
    if is_blockfrost_healthy(api):
        try:
            return api.epoch_latest().epoch
        except Exception as e:
            logging.exception(f"Error fetching current epoch number: {e}")
            return None
    else:
        logging.error("Blockfrost API is unhealthy.")
        return None


def get_epoch_start_datetime(epoch_no):
    """Get the start datetime of a given epoch."""
    logging.info(f"Fetching start datetime for epoch {epoch_no}")
    api = get_blockfrost_api()
    if is_blockfrost_healthy(api):
        try:
            epoch_data = api.epoch(number=epoch_no)
            return datetime.utcfromtimestamp(epoch_data.start_time).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        except Exception as e:
            logging.exception(
                f"Error fetching start datetime for epoch {epoch_no}: {e}"
            )
            return None
    else:
        logging.error("Blockfrost API is unhealthy.")
        return None
