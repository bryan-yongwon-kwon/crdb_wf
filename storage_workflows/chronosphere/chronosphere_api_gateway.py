import datetime
import http.client
import json
import os
from storage_workflows.logging.logger import Logger

logger = Logger()

class ChronosphereApiGateway():

    CHRONOSPHERE_API_TOKEN = os.getenv('CHRONOSPHERE_API_TOKEN')
    CHRONOSPHERE_URL = os.getenv('CHRONOSPHERE_URL')

    @staticmethod
    def create_muting_rule(label_matchers, 
                           name="Muting rule created from operator service.", 
                           starts_at=datetime.datetime.now(datetime.timezone.utc),
                           duration_hours=1) -> str:
        ends_at = starts_at + datetime.timedelta(hours=duration_hours)
        data = {
            "muting_rule": {
                "label_matchers": label_matchers,
                "name": name,
                "starts_at": starts_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "ends_at": ends_at.strftime('%Y-%m-%dT%H:%M:%SZ')
            }
        }
        muting_rule_json = json.dumps(data)
        conn = http.client.HTTPSConnection(ChronosphereApiGateway.CHRONOSPHERE_URL)
        headers = {'Content-type': 'application/json', 'Api-token': ChronosphereApiGateway.CHRONOSPHERE_API_TOKEN}
        conn.request("POST", "/api/v1/config/muting-rules", body=muting_rule_json, headers=headers)
        response = conn.getresponse()
        response_content = str(response.read().decode())
        if response.status != http.client.OK:
            raise Exception("Muting rule creation failed: {}".format(response_content))
        slug = json.loads(response_content)['muting_rule']['slug']
        logger.info("Muting rule created: {}".format(slug))
        return slug
        
    @staticmethod
    def delete_muting_rule(slug:str):
        conn = http.client.HTTPSConnection(ChronosphereApiGateway.CHRONOSPHERE_URL)
        headers = {'Content-type': 'application/json', 'Api-token': ChronosphereApiGateway.CHRONOSPHERE_API_TOKEN}
        conn.request("DELETE", "/api/v1/config/muting-rules/{}".format(slug), headers=headers)
        response = conn.getresponse()
        response_content = str(response.read().decode())
        if response.status != http.client.OK:
            raise Exception("Muting rule deletion failed: {}".format(response_content))
        logger.info("Muting rule deleted: {}".format(slug))