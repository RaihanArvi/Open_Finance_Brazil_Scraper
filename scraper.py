import json
import time
import os
import requests
import pickle
import itertools
import logging
from requests.adapters import HTTPAdapter
from tenacity import (
    retry,
    wait_exponential,
    retry_if_exception_type,
    after_log,
    stop_after_delay
)
from typing import Optional, List
from pydantic import BaseModel
from datetime import datetime

#
# Logging and Setup
#

start_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Set up logging for retry attempts
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'scraper_{start_str}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create a session with connection pooling
session = requests.Session()
adapter = HTTPAdapter(
    pool_connections=10,
    pool_maxsize=20,
    max_retries=0  # handled by tenacity.
)
session.mount('https://', adapter)
session.mount('http://', adapter)

#
# Functions
#

def send_webhook_start(time):
    try:
        requests.post("https://ntfy.sh/openfinancebrazil374628",
                     data=f"Started Scraping Open Finance Brazil Data at {time}",
                     timeout=10)
    except Exception as e:
        logger.error(f"Failed to send start webhook: {e}")

def send_webhook_finish(time):
    try:
        requests.post("https://ntfy.sh/openfinancebrazil374628",
                     data=f"Finished Scraping Open Finance Brazil Data at {time}",
                     timeout=10)
    except Exception as e:
        logger.error(f"Failed to send finish webhook: {e}")

def send_webhook_error(error_msg):
    try:
        requests.post("https://ntfy.sh/openfinancebrazil374628",
                     data=f"Error: {error_msg}",
                     timeout=10)
    except Exception as e:
        logger.error(f"Failed to send error webhook: {e}")

def send_webhook_custom(msg):
    try:
        requests.post("https://ntfy.sh/openfinancebrazil374628",
                     data=f"Message: {msg}",
                     timeout=10)
    except Exception as e:
        logger.error(f"Failed to send error webhook: {e}")

def send_webhook_progress(current, total):
    try:
        percent = (current / total) * 100
        requests.post("https://ntfy.sh/openfinancebrazil374628",
                     data=f"Progress: {current}/{total} ({percent:.1f}%)",
                     timeout=10)
    except Exception as e:
        logger.error(f"Failed to send progress webhook: {e}")

api_mailgun = ''
def send_email_message_w_attachment(message, filename):

    with open(filename, "rb") as f:
        return requests.post(
            "https://api.mailgun.net/v3/hooks.raihanarvi.com/messages",
            auth=("api", api_mailgun),
            data={
                "from": "OpenFinanceBrazil Scraper <postmaster@hooks.raihanarvi.com>",
                "to": "Raihan Adhipratama Arvi <raihanarvi62@gmail.com>",
                "subject": "OpenFinanceBrazil Milestone Data",
                "text": (
                    message
                ),
            },
            files={"attachment": (filename, f)},
        )

def send_simple_email_message(msg):
  	return requests.post(
  		"https://api.mailgun.net/v3/hooks.raihanarvi.com/messages",
  		auth=("api", api_mailgun),
  		data={"from": "OpenFinanceBrazil Scraper <postmaster@hooks.raihanarvi.com>",
			"to": "Raihan Adhipratama Arvi <raihanarvi62@gmail.com>",
  			"subject": "OpenFinanceBrazil Scraper",
  			"text": msg})

# Custom callback function that runs before each retry
def retry_callback(retry_state):
    """
    This function runs before each retry attempt.
    retry_state contains info about the retry attempt.
    """
    attempt_number = retry_state.attempt_number
    exception = retry_state.outcome.exception() if retry_state.outcome else None

    logger.info(f"Retry attempt #{attempt_number}")
    logger.info(f"Exception: {exception}")

    send_webhook_error(f"Exception: {exception}. Retry attempt #{attempt_number}")

def save_checkpoint(index, filename="checkpoint.pkl"):
    """Save current progress to resume later."""
    with open(filename, 'wb') as f:
        pickle.dump({'index': index, 'timestamp': datetime.now()}, f)
    logger.info(f"Checkpoint saved at index {index}")

def load_checkpoint(filename="checkpoint.pkl"):
    """Load the last checkpoint."""
    if os.path.exists(filename):
        with open(filename, 'rb') as f:
            checkpoint = pickle.load(f)
            logger.info(f"Resuming from checkpoint: index {checkpoint['index']}, saved at {checkpoint['timestamp']}")
            return checkpoint['index']
    return 0

# Looped for each year.
@retry(
    wait=wait_exponential(multiplier=1, min=1, max=60),
    stop=stop_after_delay(300),
    retry=retry_if_exception_type((requests.exceptions.Timeout,
                                   requests.exceptions.ConnectionError,
                                   requests.exceptions.HTTPError,
                                   requests.exceptions.RequestException
                                   )),
    before_sleep=retry_callback,
    after=after_log(logger, logging.INFO)
)
def get_response(pairs, api, endpoint, status, dates: List[str]):
    url = "https://dashboard.openfinancebrasil.org.br/api/api-requests"

    payload = {
        "axis": "date",
        "dates": dates,
        "phase": "transactional-data",

        "receivers": [pairs[0]['value']],
        "transmitters": [pairs[1]['value']],
        "apis": [api['value']],
        "endpoints": [endpoint['value']],
        "status": status
    }

    headers = {
        "Content-Type": "application/json",
        "Origin": "https://dashboard.openfinancebrasil.org.br",
        "User-Agent": "Mozilla/5.0"
    }

    response = session.post(url, json=payload, headers=headers, timeout=50)

    # Raise exception for rate limiting or server errors to trigger retry.
    if response.status_code in [429, 500, 502, 503, 504]:
        logger.warning(f"Received status code {response.status_code}, will retry")
        response.raise_for_status()

    return response

#
# Data
#

dates_2023 = ["2023-01-06T00:00:00.000Z","2023-01-13T00:00:00.000Z","2023-01-20T00:00:00.000Z","2023-01-27T00:00:00.000Z","2023-02-03T00:00:00.000Z","2023-02-10T00:00:00.000Z","2023-02-17T00:00:00.000Z","2023-02-24T00:00:00.000Z","2023-03-03T00:00:00.000Z","2023-03-10T00:00:00.000Z","2023-03-17T00:00:00.000Z","2023-03-24T00:00:00.000Z","2023-03-31T00:00:00.000Z","2023-04-07T00:00:00.000Z","2023-04-14T00:00:00.000Z","2023-04-21T00:00:00.000Z","2023-04-28T00:00:00.000Z","2023-05-05T00:00:00.000Z","2023-05-12T00:00:00.000Z","2023-05-19T00:00:00.000Z","2023-05-26T00:00:00.000Z","2023-06-02T00:00:00.000Z","2023-06-09T00:00:00.000Z","2023-06-16T00:00:00.000Z","2023-06-23T00:00:00.000Z","2023-06-30T00:00:00.000Z","2023-07-07T00:00:00.000Z","2023-07-14T00:00:00.000Z","2023-07-21T00:00:00.000Z","2023-07-28T00:00:00.000Z","2023-08-04T00:00:00.000Z","2023-08-11T00:00:00.000Z","2023-08-18T00:00:00.000Z","2023-08-25T00:00:00.000Z","2023-09-01T00:00:00.000Z","2023-09-08T00:00:00.000Z","2023-09-15T00:00:00.000Z","2023-09-22T00:00:00.000Z","2023-09-29T00:00:00.000Z","2023-10-06T00:00:00.000Z","2023-10-13T00:00:00.000Z","2023-10-20T00:00:00.000Z","2023-10-27T00:00:00.000Z","2023-11-03T00:00:00.000Z","2023-11-10T00:00:00.000Z","2023-11-17T00:00:00.000Z","2023-11-24T00:00:00.000Z","2023-12-01T00:00:00.000Z","2023-12-08T00:00:00.000Z","2023-12-15T00:00:00.000Z","2023-12-22T00:00:00.000Z","2023-12-29T00:00:00.000Z"]
dates_2024 = ["2024-01-05T00:00:00.000Z","2024-01-12T00:00:00.000Z","2024-01-19T00:00:00.000Z","2024-01-26T00:00:00.000Z","2024-02-02T00:00:00.000Z","2024-02-09T00:00:00.000Z","2024-02-16T00:00:00.000Z","2024-02-23T00:00:00.000Z","2024-03-01T00:00:00.000Z","2024-03-08T00:00:00.000Z","2024-03-15T00:00:00.000Z","2024-03-22T00:00:00.000Z","2024-03-29T00:00:00.000Z","2024-04-05T00:00:00.000Z","2024-04-12T00:00:00.000Z","2024-04-19T00:00:00.000Z","2024-04-26T00:00:00.000Z","2024-05-03T00:00:00.000Z","2024-05-10T00:00:00.000Z","2024-05-17T00:00:00.000Z","2024-05-24T00:00:00.000Z","2024-05-31T00:00:00.000Z","2024-06-07T00:00:00.000Z","2024-06-14T00:00:00.000Z","2024-06-21T00:00:00.000Z","2024-06-28T00:00:00.000Z","2024-07-05T00:00:00.000Z","2024-07-12T00:00:00.000Z","2024-07-19T00:00:00.000Z","2024-07-26T00:00:00.000Z","2024-08-02T00:00:00.000Z","2024-08-09T00:00:00.000Z","2024-08-16T00:00:00.000Z","2024-08-23T00:00:00.000Z","2024-08-30T00:00:00.000Z","2024-09-06T00:00:00.000Z","2024-09-13T00:00:00.000Z","2024-09-20T00:00:00.000Z","2024-09-27T00:00:00.000Z","2024-10-04T00:00:00.000Z","2024-10-11T00:00:00.000Z","2024-10-18T00:00:00.000Z","2024-10-25T00:00:00.000Z","2024-11-01T00:00:00.000Z","2024-11-08T00:00:00.000Z","2024-11-15T00:00:00.000Z","2024-11-22T00:00:00.000Z","2024-11-29T00:00:00.000Z","2024-12-06T00:00:00.000Z","2024-12-13T00:00:00.000Z","2024-12-20T00:00:00.000Z","2024-12-27T00:00:00.000Z"]
dates_2025 = ["2025-01-03T00:00:00.000Z","2025-01-10T00:00:00.000Z","2025-01-17T00:00:00.000Z","2025-01-24T00:00:00.000Z","2025-01-31T00:00:00.000Z","2025-02-07T00:00:00.000Z","2025-02-14T00:00:00.000Z","2025-02-21T00:00:00.000Z","2025-02-28T00:00:00.000Z","2025-03-07T00:00:00.000Z","2025-03-14T00:00:00.000Z","2025-03-21T00:00:00.000Z","2025-03-28T00:00:00.000Z","2025-04-04T00:00:00.000Z","2025-04-11T00:00:00.000Z","2025-04-18T00:00:00.000Z","2025-04-25T00:00:00.000Z","2025-05-02T00:00:00.000Z","2025-05-09T00:00:00.000Z","2025-05-16T00:00:00.000Z","2025-05-23T00:00:00.000Z","2025-05-30T00:00:00.000Z","2025-06-06T00:00:00.000Z","2025-06-13T00:00:00.000Z","2025-06-20T00:00:00.000Z","2025-06-27T00:00:00.000Z","2025-07-04T00:00:00.000Z","2025-07-11T00:00:00.000Z","2025-07-18T00:00:00.000Z","2025-07-25T00:00:00.000Z","2025-08-01T00:00:00.000Z","2025-08-08T00:00:00.000Z","2025-08-15T00:00:00.000Z","2025-08-22T00:00:00.000Z","2025-08-29T00:00:00.000Z","2025-09-05T00:00:00.000Z","2025-09-12T00:00:00.000Z","2025-09-19T00:00:00.000Z","2025-09-26T00:00:00.000Z","2025-10-03T00:00:00.000Z","2025-10-10T00:00:00.000Z","2025-10-17T00:00:00.000Z","2025-10-24T00:00:00.000Z","2025-10-31T00:00:00.000Z","2025-11-07T00:00:00.000Z","2025-11-14T00:00:00.000Z","2025-11-21T00:00:00.000Z","2025-11-28T00:00:00.000Z","2025-12-05T00:00:00.000Z","2025-12-12T00:00:00.000Z","2025-12-19T00:00:00.000Z","2025-12-26T00:00:00.000Z"]
list_of_dates = [dates_2023, dates_2024, dates_2025]

file_apis = "payloads/apis.json"
with open(file_apis, "r") as f:
    apis = json.load(f)

file_endpoints = "payloads/endpoints.json"
with open(file_endpoints, "r") as f:
    endpoints = json.load(f)

file_organizations = "payloads/organizations.json"
with open(file_organizations, "r") as f:
    organizations = json.load(f)

organizations_pairs = list(itertools.product(organizations, organizations))

# Status; 200 = success, 500 = fail.
statuses = [500,200]

#
# Model
#

class APIRequestDataPoint(BaseModel):
    _id: str
    total: int
    date: str

class APIRequestCombination(BaseModel):
    index: int = None
    receiver: str = None
    transmitter: str = None
    api: str = None
    endpoint: str = None
    status: int = None

    response_status_codes: List[int] = None # expect 3 values, each corresponds to [2023, 2024, 2025] correspondingly.
    data_points: Optional[List[APIRequestDataPoint]] = None

class APIRequestAllCombinations(BaseModel):
    api_requests: List[APIRequestCombination] = None

#
# Main Loop
#

send_webhook_start(start_str)

requests_list = []

combinations = list(itertools.product(
    organizations_pairs,
    apis,
    endpoints,
    statuses
))

total_combinations = len(combinations)
logger.info(f"Total combinations to process: {total_combinations}")

# Load checkpoint if exists
start_index = load_checkpoint()

send_webhook_custom(f"Starting to process combinations from index {start_index} out of {total_combinations} total combinations at {start_str}.")
logger.info(f"Starting to process combinations from index {start_index} out of {total_combinations} total combinations at {start_str}.")

last_idx = start_index

try:
    for idx, (pair, api, endpoint, status) in enumerate(combinations[start_index:], start=start_index):
        try:
            logger.info(f"Processing {idx+1}/{total_combinations}: {pair[0]['label']} -> {pair[1]['label']}, {api['label']}, {endpoint['label']}, status={status}")

            values = []
            response_codes = []
            for dates in list_of_dates:
                response = get_response(pair, api, endpoint, status, dates)

                response_codes.append(response.status_code)
                values = values + response.json()

            api_request = APIRequestCombination(
                index = idx,
                receiver = pair[0]['label'],
                transmitter = pair[1]['label'],
                api = api['label'],
                endpoint = endpoint['label'],
                status = status,

                response_status_codes = response_codes,
                data_points = values,
            )

            requests_list.append(api_request)

            # Save checkpoint every time.
            save_checkpoint(idx + 1)
            # Save intermediate results
            output_temp = APIRequestAllCombinations(api_requests=requests_list)
            temp_file = f"temp/open_finance_brazil_temp-{start_str}.json"
            with open(temp_file, "w") as f:
                f.write(output_temp.model_dump_json(indent=2))

            # Send milestone notifications every 1000 iterations.
            if (idx + 1) % 1000 == 0:
                send_webhook_progress(idx + 1, total_combinations)

            # Send progress at specific percentages.
            percent_complete = ((idx + 1) / total_combinations) * 100
            if int(percent_complete) in [5,10,15,20,25,35,50,60,75,85,90,95,98]:
                send_webhook_progress(idx + 1, total_combinations)

            # Send temp every 15000 iterations.
            if (idx + 1) % 15000 == 0:
                cst_msg = f"OpenFinanceBrazil Milestone Temporary Data: {idx+1}/{total_combinations} combinations processed. File is attached."
                send_email_message_w_attachment(cst_msg, temp_file)


        except KeyboardInterrupt:
            logger.warning("KeyboardInterrupt received, saving progress...")

            last_idx = idx
            save_checkpoint(idx)
            raise

        except Exception as e:
            logger.error(f"Error processing combination {idx}: {e}", exc_info=True)
            send_webhook_error(f"Error processing combination {idx}: {e}. Stopping.")
            send_simple_email_message(f"Error processing combination {idx}: {e}. Stopping.")

            last_idx = idx
            save_checkpoint(idx)
            raise


except KeyboardInterrupt:
    logger.warning("Script interrupted by user")
except Exception as e:
    logger.error(f"Fatal error in main loop: {e}", exc_info=True)
finally:
    # save final output
    if requests_list:
        output = APIRequestAllCombinations(api_requests=requests_list)
        finish_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_file = f"output/open_finance_brazil-{finish_time}-processed-{last_idx}-out_of-{total_combinations}.json"

        with open(output_file, "w") as f:
            f.write(output.model_dump_json(indent=2))

        logger.info(f"Final output saved to {output_file}")
        logger.info(f"Successfully processed {len(requests_list)} combinations")

        send_webhook_custom(f"Successfully processed {len(requests_list)} combinations out of {total_combinations} total combinations. Saved to {output_file} at {finish_time}.")
        send_email_message_w_attachment(f"Successfully processed {len(requests_list)} combinations out of {total_combinations} total combinations. Saved to {output_file} at {finish_time}.", output_file)

        send_webhook_finish(finish_time)
    else:
        logger.warning("No data collected")
