import datetime
import http.client
import json
import os
from datetime import datetime as dt
from storage_workflows.chronosphere.muting_rule_not_found_exception import MutingRuleNotFoundException
from storage_workflows.logging.logger import Logger

logger = Logger()

class ChronosphereApiGateway():

    CHRONOSPHERE_API_TOKEN = os.getenv('CHRONOSPHERE_API_TOKEN')
    CHRONOSPHERE_URL = os.getenv('CHRONOSPHERE_URL')

    @staticmethod
    def create_muting_rule(label_matchers, 
                           name="Muting rule created from operator service.", 
                           starts_at=dt.utcnow(),
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
        if not ChronosphereApiGateway.muting_rule_exist(slug):
            logger.info("Muting rule does not exist. Skip deletion.")
            return
        conn = http.client.HTTPSConnection(ChronosphereApiGateway.CHRONOSPHERE_URL)
        headers = {'Content-type': 'application/json', 'Api-token': ChronosphereApiGateway.CHRONOSPHERE_API_TOKEN}
        conn.request("DELETE", "/api/v1/config/muting-rules/{}".format(slug), headers=headers)
        response = conn.getresponse()
        response_content = str(response.read().decode())
        if response.status != http.client.OK:
            raise Exception("Muting rule deletion failed: {}".format(response_content))
        logger.info("Muting rule deleted: {}".format(slug))

    @staticmethod
    def update_muting_rule(create_if_missing:bool,
                           slug: str,
                           name: str,
                           label_matchers: dict,
                           starts_at: str,
                           ends_at: str,
                           comment: str = ""):
        if not ChronosphereApiGateway.muting_rule_exist(slug):
            logger.info("Muting rule does not exist. Skip updating.")
            return
        conn = http.client.HTTPSConnection(ChronosphereApiGateway.CHRONOSPHERE_URL)
        headers = {'Content-type': 'application/json', 'Api-token': ChronosphereApiGateway.CHRONOSPHERE_API_TOKEN}
        data = {
                    "create_if_missing": create_if_missing,
                    "muting_rule": {
                        "comment": comment,
                        "ends_at": ends_at,
                        "label_matchers": label_matchers,
                        "name": name,
                        "slug": slug,
                        "starts_at": starts_at
                    }
                }
        muting_rule_json = json.dumps(data)
        conn.request("PUT", "/api/v1/config/muting-rules/{}".format(slug), 
                     headers=headers,
                     body=muting_rule_json)
        response = conn.getresponse()
        response_content = str(response.read().decode())
        if response.status != http.client.OK:
            raise Exception("Muting rule updating failed: {}".format(response_content))
        logger.info("Muting rule updated: {}".format(slug))
        return json.loads(response_content)['muting_rule']
    
    @staticmethod
    def read_muting_rule(slug: str):
        conn = http.client.HTTPSConnection(ChronosphereApiGateway.CHRONOSPHERE_URL)
        headers = {'Content-type': 'application/json', 'Api-token': ChronosphereApiGateway.CHRONOSPHERE_API_TOKEN}
        conn.request("GET", "/api/v1/config/muting-rules/{}".format(slug), headers=headers)
        response = conn.getresponse()
        response_content = str(response.read().decode())
        if response.status != http.client.OK:
            NOT_FOUND_ERR_MSG = "category=INVALID_REQUEST_ERROR code=NOT_FOUND"
            if NOT_FOUND_ERR_MSG in response_content["message"]:
                raise MutingRuleNotFoundException
            raise Exception("Muting rule reading failed: {}".format(response_content))
        return json.loads(response_content)['muting_rule']
    
    @staticmethod
    def muting_rule_expired(slug: str):
        response = ChronosphereApiGateway.read_muting_rule(slug)
        ends_at = response["ends_at"]
        format = "%Y-%m-%dT%H:%M:%SZ"
        ends_at_time = dt.strftime(ends_at, format)
        return ends_at_time < dt.utcnow()
    
    @staticmethod
    def muting_rule_exist(slug:str):
        try:
            ChronosphereApiGateway.read_muting_rule(slug)
        except MutingRuleNotFoundException:
            return False
        return True