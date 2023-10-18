import os
import json
from requests import get, post, exceptions
from requests.cookies import RequestsCookieJar
from storage_workflows.logging.logger import Logger
from urllib.parse import quote

logger = Logger()


class CrdbApiGateway:

    @staticmethod
    def login():
        rootpwd = os.getenv('ROOT_PASSWORD')
        encoded_rootpwd = quote(rootpwd)
        url = f"https://{CrdbApiGateway.__make_url()}/api/v2/login/?username=root&password={encoded_rootpwd}"

        try:
            response = post(url)

            # Check for 401 Unauthorized
            if response.status_code == 401:
                logger.error(f"Login failed with 401 Unauthorized. Message: {response.text}")
                return None

            # Check if the response is not 200 OK
            elif response.status_code != 200:
                logger.error(f"Login failed with status code {response.status_code}: {response.text}")
                return None

            # Check for empty response
            if not response.text.strip():
                logger.error(f"Empty response received from login url: {url}")
                return None

            session = response.json().get("session")
            return session

        except json.decoder.JSONDecodeError:
            logger.error(f"Cannot retrieve session token from login url: {url}. Response: {response.text}")
        except exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")

        return None

    @staticmethod
    def list_nodes(session: str, limit=200, offset=0):
        try:
            response = get(f"https://{CrdbApiGateway.__make_url()}/api/v2/nodes/?limit={limit}&offset={offset}",
                           headers={"X-Cockroach-API-Session": session})

            # Add a check for the status code here too, if required.

            return response.json()
        except json.decoder.JSONDecodeError:
            logger.error(f"Cannot retrieve node list. Response: {response.text}")
        except exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")

        return {}

    @staticmethod
    def get_node_details_from_endpoint(session: str, node_id: str):
        jar = RequestsCookieJar()
        jar.set(name='session', value=session, path='/')
        try:
            response = get(f"https://{CrdbApiGateway.__make_url()}/_status/nodes/{node_id}",
                           cookies=jar)

            # Add a check for the status code here too, if required.

            return response.json()
        except json.decoder.JSONDecodeError:
            logger.error(f"Cannot retrieve details for node {node_id}. Response: {response.text}")
        except exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")

        return {}

    @staticmethod
    def __make_url():
        cluster_name = os.getenv('CLUSTER_NAME').replace("_", "-")
        staging_url = f"{cluster_name}-crdb-admin.doorcrawl-int.com"
        prod_url = f"{cluster_name}-crdb-admin.doordash-int.com"
        if os.getenv('DEPLOYMENT_ENV') == "staging":
            return staging_url
        return prod_url
