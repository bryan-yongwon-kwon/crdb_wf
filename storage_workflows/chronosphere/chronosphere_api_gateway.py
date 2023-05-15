import datetime
import http.client
import json
import os

class ChronosphereApiGateway():

    @staticmethod
    def create_muting_rule(self, label_matchers, name="Muting rule created from operator service.", starts_at=datetime.datetime.now(datetime.timezone.utc), path="/api/v1/config/muting-rules", http_method="POST"):
        #rules expire in 1 hour
        ends_at = starts_at + datetime.timedelta(hours=1)
        chronosphere_api_token = os.getenv('CHRONOSPHERE_API_TOKEN')
        chronosphere_url = os.getenv('CHRONOSPHERE_URL')
        # Define the muting rule parameters
        data = {
            "muting_rule": {
                "label_matchers": label_matchers,
                "name": name,
                "starts_at": starts_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "ends_at": ends_at.strftime('%Y-%m-%dT%H:%M:%SZ')
            }
        }
        muting_rule_json = json.dumps(data)

        # Make the API request
        conn = http.client.HTTPSConnection(chronosphere_url)
        headers = {'Content-type': 'application/json', 'Api-token': chronosphere_api_token}
        conn.request(http_method, path, body=muting_rule_json, headers=headers)
        response = conn.getresponse()
        if response.status != http.client.OK:
            raise Exception("Chronosphere API to mute alerts failure details "+ str(response.read().decode()))
        
        