import time
import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
    "preview": PREVIEW_EXPLORER_URL
}

def get_epoch_start_datetime_from_explorer(env, epoch_no):
    """
    Fetches the start datetime of a specific epoch from the Cardano explorer."""
    headers = {'Content-type': 'application/json'}
    payload = (
        f'''{{"query":"query searchForEpochByNumber($number: Int!) {{\n'''
        f'''  epochs(where: {{number: {{_eq: $number}}}}) {{\n'''
        f'''    ...EpochOverview\n'''
        f'''  }}\n'''
        f'''}}\n\n'''
        f'''fragment EpochOverview on Epoch {{\n'''
        f'''  blocks(limit: 1) {{\n'''
        f'''    protocolVersion\n'''
        f'''  }}\n'''
        f'''  blocksCount\n'''
        f'''  lastBlockTime\n'''
        f'''  number\n'''
        f'''  startedAt\n'''
        f'''  output\n'''
        f'''  transactionsCount\n'''
        f'''}}\n", "variables":{{"number":{epoch_no}}}}'''
    )

    url = EXPLORER_URLS.get(env)

    if url is None:
        logging.error(f"The provided 'env' is not supported. Please use one of: {', '.join(EXPLORER_URLS.keys())}")
        return None

    result = None
    try:
        response = requests.post(url, data=payload, headers=headers)
        status_code = response.status_code

        if status_code != 200:
            logging.error(f"Failed to fetch data from {url}: {response.text}")
            logging.error(f"!!! ERROR: status_code != 200 when getting start time for epoch {epoch_no} on {env}")
        else:
            count = 0
            while "data" in response.json() and response.json().get('data') is None:
                logging.info(f"Attempt {count}: Response is None. Retrying...")
                time.sleep(30)
                response = requests.post(url, data=payload, headers=headers)
                count += 1

                if count > 10:
                    logging.error(f"!!! ERROR: Not able to get start time for epoch {epoch_no} on {env} after 10 tries")
                    break

            else:
                data = response.json()
                result = data['data']['epochs'][0]['startedAt']

    except requests.RequestException as e:
        logging.exception(f"Request failed: {e}")
    except KeyError:
        logging.error(f"Unexpected response structure: {response.json()}")

    return result
