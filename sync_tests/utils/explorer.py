import json
import logging
import time

import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

MAINNET_EXPLORER_URL = "https://explorer.cardano.org/graphql"
STAGING_EXPLORER_URL = "https://explorer.staging.cardano.org/graphql"
TESTNET_EXPLORER_URL = "https://explorer.cardano-testnet.iohkdev.io/graphql"
SHELLEY_QA_EXPLORER_URL = "https://explorer.shelley-qa.dev.cardano.org/graphql"
PREPROD_EXPLORER_URL = None
PREVIEW_EXPLORER_URL = None

EXPLORER_URLS = {
    "mainnet": MAINNET_EXPLORER_URL,
    "staging": STAGING_EXPLORER_URL,
    "testnet": TESTNET_EXPLORER_URL,
    "shelley-qa": SHELLEY_QA_EXPLORER_URL,
    "preprod": PREPROD_EXPLORER_URL,
    "preview": PREVIEW_EXPLORER_URL,
}


def get_epoch_start_datetime_from_explorer(env, epoch_no):
    """Fetches the start datetime of a specific epoch from the Cardano explorer."""
    headers = {"Content-type": "application/json"}
    query = """
    query searchForEpochByNumber($number: Int!) {
      epochs(where: {number: {_eq: $number}}) {
        ...EpochOverview
      }
    }

    fragment EpochOverview on Epoch {
      blocks(limit: 1) {
        protocolVersion
      }
      blocksCount
      lastBlockTime
      number
      startedAt
      output
      transactionsCount
    }
    """

    variables = {"number": epoch_no}

    payload = json.dumps({"query": query, "variables": variables})

    url = EXPLORER_URLS.get(env)

    if url is None:
        logging.error(
            f"The provided 'env' is not supported. Please use one of: {', '.join(EXPLORER_URLS.keys())}"
        )
        return None

    result = None
    try:
        response = requests.post(url, data=payload, headers=headers)
        status_code = response.status_code

        if status_code != 200:
            logging.error(f"Failed to fetch data from {url}: {response.text}")
            logging.error(
                f"!!! ERROR: status_code != 200 when getting start time for epoch {epoch_no} on {env}"
            )
        else:
            count = 0
            while "data" in response.json() and response.json().get("data") is None:
                logging.info(f"Attempt {count}: Response is None. Retrying...")
                time.sleep(30)
                response = requests.post(url, data=payload, headers=headers)
                count += 1

                if count > 10:
                    logging.error(
                        f"!!! ERROR: Not able to get start time for epoch {epoch_no} on {env} after 10 tries"
                    )
                    break

            else:
                data = response.json()
                result = data["data"]["epochs"][0]["startedAt"]

    except requests.RequestException as e:
        logging.exception(f"Request failed: {e}")
    except KeyError:
        logging.exception(f"Unexpected response structure: {response.json()}")

    return result
