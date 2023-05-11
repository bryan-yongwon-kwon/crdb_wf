import http.client
import json

CHRONOSPHERE_API_TOKEN="cd1bf1bf8bfb8fa8a932ee136f11f78a121369f84a9ee5acff3542abc09bd1c2"

class ChronosphereClient():
    def __init__(self, api_token=CHRONOSPHERE_API_TOKEN, url="doordash.chronosphere.io"):
        self.api_token = api_token
        self.url = url

    def create_muting_rule(self, label_matchers, name, starts_at, ends_at, path="/api/v1/config/muting-rules", http_method="POST"):
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
        conn = http.client.HTTPSConnection(self.url)
        headers = {'Content-type': 'application/json', 'Api-token': self.api_token}
        conn.request(http_method, path, body=muting_rule_json, headers=headers)
        response = conn.getresponse()
        print(response.read().decode())