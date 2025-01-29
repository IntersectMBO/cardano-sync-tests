import datetime
import logging
import os

from blockfrost import BlockFrostApi

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_blockfrost_api() -> BlockFrostApi:
    """Initialize and return the BlockFrost API client."""
    project_id = os.environ.get("BLOCKFROST_API_KEY")
    if not project_id:
        msg = "BLOCKFROST_API_KEY is not set."
        raise ValueError(msg)
    return BlockFrostApi(project_id=project_id)


def is_blockfrost_healthy(api: BlockFrostApi) -> bool:
    """Check if the Blockfrost API is healthy."""
    try:
        health = api.health(return_type="json")
        return bool(health.get("is_healthy", False))
    except Exception:
        logging.exception("Error checking Blockfrost health")
        return False


def get_tx_count_per_epoch(epoch_no: int) -> int | None:
    """Get the transaction count for a given epoch."""
    logging.info(f"Fetching transaction count for epoch {epoch_no}")
    api = get_blockfrost_api()
    if is_blockfrost_healthy(api):
        try:
            return int(api.epoch(number=epoch_no).tx_count)
        except Exception:
            logging.exception(f"Error fetching transaction count for epoch {epoch_no}")
            return None
    else:
        logging.error("Blockfrost API is unhealthy.")
        return None


def get_current_epoch_no() -> int | None:
    """Get the current epoch number."""
    logging.info("Fetching current epoch number")
    api = get_blockfrost_api()
    if is_blockfrost_healthy(api):
        try:
            return int(api.epoch_latest().epoch)
        except Exception:
            logging.exception("Error fetching current epoch number")
            return None
    else:
        logging.error("Blockfrost API is unhealthy.")
        return None


def get_epoch_start_datetime(epoch_no: int) -> str | None:
    """Get the start datetime of a given epoch."""
    logging.info(f"Fetching start datetime for epoch {epoch_no}")
    api = get_blockfrost_api()
    if is_blockfrost_healthy(api):
        try:
            epoch_data = api.epoch(number=epoch_no)
            return datetime.datetime.fromtimestamp(
                epoch_data.start_time, tz=datetime.timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            logging.exception(f"Error fetching start datetime for epoch {epoch_no}")
            return None
    else:
        logging.error("Blockfrost API is unhealthy.")
        return None
