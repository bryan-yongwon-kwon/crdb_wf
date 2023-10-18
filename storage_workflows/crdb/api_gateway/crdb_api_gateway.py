import os
import json
from requests import get, post, exceptions
from storage_workflows.logging.logger import Logger
from urllib.parse import quote
from requests.cookies import RequestsCookieJar

logger = Logger()

class CrdbApiGateway:

    @staticmethod
    def login(retries=2):
        rootpwd = os.getenv('ROOT_PASSWORD')
        encoded_rootpwd = quote(rootpwd)
        url = f"https://{CrdbApiGateway.__make_url()}/api/v2/login/?username=root&password={encoded_rootpwd}"

        for _ in range(retries + 1): # retries + original attempt
            try:
                response = post(url)

                if response.status_code == 200 and response.text.strip():
                    return response.json().get("session")

                # Log the unexpected response
                logger.warning(f"Unexpected response from login URL {url}: {response.text}")

            except (json.decoder.JSONDecodeError, exceptions.RequestException) as e:
                logger.error(f"Request failed: {e}")

        logger.error(f"Login failed after {retries + 1} attempts")
        return None

    @staticmethod
    def list_nodes(session:str, limit=200, offset=0, retries=2):
        for _ in range(retries + 1):
            try:
                response = get(f"https://{CrdbApiGateway.__make_url()}/api/v2/nodes/?limit={limit}&offset={offset}",
                               headers={"X-Cockroach-API-Session": session})

                if response.status_code == 200 and response.text.strip():
                    return response.json()

                logger.warning(f"Unexpected response from list nodes URL: {response.text}")

            except (json.decoder.JSONDecodeError, exceptions.RequestException) as e:
                logger.error(f"Request failed: {e}")

        logger.error(f"Failed to retrieve node list after {retries + 1} attempts")
        return {}

    @staticmethod
    def get_node_details_from_endpoint(session:str, node_id:str, retries=2):
        jar = RequestsCookieJar()
        jar.set(name='session', value=session, path='/')

        for _ in range(retries + 1):
            try:
                response = get(f"https://{CrdbApiGateway.__make_url()}/_status/nodes/{node_id}",
                               cookies=jar)

                if response.status_code == 200 and response.text.strip():
                    return response.json()

                logger.warning(f"Unexpected response from node details URL for node {node_id}: {response.text}")

            except (json.decoder.JSONDecodeError, exceptions.RequestException) as e:
                logger.error(f"Request failed: {e}")

        logger.error(f"Failed to retrieve details for node {node_id} after {retries + 1} attempts")
        return {}

    @staticmethod
    def __make_url():
        cluster_name = os.getenv('CLUSTER_NAME').replace("_", "-")
        staging_url = f"{cluster_name}-crdb-admin.doorcrawl-int.com"
        prod_url = f"{cluster_name}-crdb-admin.doordash-int.com"
        if os.getenv('DEPLOYMENT_ENV') == "staging":
            return staging_url
        return prod_url
