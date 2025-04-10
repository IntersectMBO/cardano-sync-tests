import json
import logging
import time

import requests

LOGGER = logging.getLogger(__name__)

MAINNET_EXPLORER_URL = "https://explorer.cardano.org/graphql"
STAGING_EXPLORER_URL = "https://explorer.staging.cardano.org/graphql"
TESTNET_EXPLORER_URL = "https://explorer.cardano-testnet.iohkdev.io/graphql"
SHELLEY_QA_EXPLORER_URL = "https://explorer.shelley-qa.dev.cardano.org/graphql"
PREPROD_EXPLORER_URL = None
PREVIEW_EXPLORER_URL = "https://preview.koios.rest/api/v1/epoch_info?_epoch_no="

EXPLORER_URLS = {
    "mainnet": MAINNET_EXPLORER_URL,
    "staging": STAGING_EXPLORER_URL,
    "testnet": TESTNET_EXPLORER_URL,
    "shelley-qa": SHELLEY_QA_EXPLORER_URL,
    "preprod": PREPROD_EXPLORER_URL,
    "preview": PREVIEW_EXPLORER_URL,
}


def get_epoch_start_datetime_from_explorer(env: str, epoch_no: int) -> str | None:
    """Fetch the start datetime of a specific epoch from the Cardano explorer."""
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
        LOGGER.error(
            "The provided 'env' is not supported. Please use one of: "
            f"{', '.join(EXPLORER_URLS.keys())}"
        )
        return None

    result = None
    try:
        if env == "preview":
            url = f"{url}{epoch_no}"
            response = requests.get(url=url, headers=headers)

            if response.status_code != 200:
                LOGGER.error(f"Failed to fetch data from {url}: {response.text}")
                LOGGER.error(
                    "!!! ERROR: status_code != 200 when getting start time for "
                    f"epoch {epoch_no} on {env}"
                )
            else:
                result = response.json()[0]["start_time"]
        else:
            response = requests.post(url, data=payload, headers=headers)
            status_code = response.status_code

            if status_code != 200:
                LOGGER.error(f"Failed to fetch data from {url}: {response.text}")
                LOGGER.error(
                    "!!! ERROR: status_code != 200 when getting start time for "
                    f"epoch {epoch_no} on {env}"
                )
            else:
                count = 0
                while "data" in response.json() and response.json().get("data") is None:
                    LOGGER.info(f"Attempt {count}: Response is None. Retrying...")
                    time.sleep(30)
                    response = requests.post(url, data=payload, headers=headers)
                    count += 1

                    if count > 10:
                        LOGGER.error(
                            "!!! ERROR: Not able to get start time for "
                            f"epoch {epoch_no} on {env} after 10 tries"
                        )
                        break

                else:
                    data = response.json()
                    result = data["data"]["epochs"][0]["startedAt"]

    except requests.RequestException:
        LOGGER.exception("Request failed")
    except KeyError:
        LOGGER.exception(f"Unexpected response structure: {response.json()}")

    return result
