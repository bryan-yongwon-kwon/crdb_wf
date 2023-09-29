import json
import csv
import requests
import os
from requests import post
from storage_workflows.logging.logger import Logger
from datetime import datetime

logger = Logger()


def generate_csv_file(list_of_values, header_fields):
    """
    Generate a CSV file from list_of_values based on specified header fields.

    Inputs:
    - list_of_values: A list of objects or dictionaries. Each item represents a check and should have attributes/keys
              that correspond to the provided header fields.
    - header_fields: A list of strings specifying the order and name of columns in the CSV. Each string
                     should be a valid attribute/key of a check in the checks list.

    Output:
    - filename: A string indicating the path to the generated CSV file.

    Example Usage:
    header = ["cluster_name", "check_type", "check_result", "check_output"]
    list_of_values = [...]  # This should be a list of objects with attributes that match the header fields or
                                dictionaries with matching keys.
    filename = self.generate_csv_file(checks, header)
    """

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"/tmp/failed_checks_{timestamp}.csv"

    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        # Writing the header
        writer.writerow(header_fields)

        for check in list_of_values:
            # Assuming checks are objects, use getattr to fetch attributes.
            # If checks are dictionaries, replace getattr(check, field) with check[field]
            if getattr(check, header_fields[0]) == 'test_prod':
                continue
            writer.writerow([getattr(check, field) for field in header_fields])

    return filename


def send_to_slack(deployment_env, message):
    if deployment_env == 'prod':
        slack_webhook_url = os.getenv('SLACK_WEBHOOK_STORAGE_ALERTS_CRDB')
    elif deployment_env == 'staging':
        slack_webhook_url = os.getenv('SLACK_WEBHOOK_STORAGE_ALERTS_CRDB_STAGING')
    else:
        slack_webhook_url = os.getenv('SLACK_WEBHOOK_STORAGE_ALERT_TEST')
    headers = {
        'Content-Type': 'application/json'
    }
    data = {
        'text': message
    }
    response = requests.post(slack_webhook_url, headers=headers, data=json.dumps(data))
    return response.status_code


class SlackNotification:

    def __init__(self, webhook_url, bearer_token=None):
        self.__webhook_url = webhook_url
        self.__bearer_token = bearer_token

    def send_notification(self, notification_content: dict):
        response = post(self.__webhook_url,
                        headers={"Content-type": 'application/json'},
                        data=json.dumps(notification_content))
        logger.info(response.text)

    def send_to_slack_with_attachment(self, filename, message, channel):
        url = "https://slack.com/api/files.upload"
        headers = {
            'Authorization': f'Bearer {self.__bearer_token}',
        }

        # Check the bearer token:
        logger.info(f"Bearer Token: {self.__bearer_token}")

        # Check the file existence:
        if not os.path.exists(filename):
            logger.error(f"File {filename} does not exist!")
            return -1  # Indicate file not found.

        with open(filename, 'rb') as f:
            payload = {
                "channels": f"#{channel}",
                "file": f,
                "initial_comment": message,
            }

            try:
                response = requests.post(url, headers=headers, files=payload)
                # Log the full response for debugging:
                logger.info(f"Slack Response: {response.text}")
            except Exception as e:
                # Log any exception that arises:
                logger.error(f"Error sending file to Slack: {str(e)}")
                return -2  # Indicate error in sending file.

        return response.status_code

