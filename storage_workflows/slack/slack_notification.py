import json
from requests import post
from storage_workflows.logging.logger import Logger

logger = Logger()

class SlackNotification:

    def __init__(self, webhook_url):
        self.__webhook_url = webhook_url

    def send_notification(self, notification_content: dict):
        response = post(self.__webhook_url, 
             headers={"Content-type": 'application/json'},
             data=json.dumps(notification_content))
        logger.info(response.text)
