import os, json
from requests import get, post, cookies, exceptions
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

            session = response.json().get("session")
            return session

        except json.decoder.JSONDecodeError:
            logger.error(f"Cannot retrieve session token from login url: {url}")
        except exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")

        return None

    
    @staticmethod
    def list_nodes(session:str, limit=200, offset=0):
        response = get("https://{}/api/v2/nodes/?limit={}&offset={}".format(CrdbApiGateway.__make_url(), limit, offset),
            headers={"X-Cockroach-API-Session": session}).json()
        return response
    
    @staticmethod
    def get_node_details_from_endpoint(session:str, node_id:str):
        jar = cookies.RequestsCookieJar()
        jar.set(name='session', value=session, path='/')
        response = get("https://{}/_status/nodes/{}".format(CrdbApiGateway.__make_url(), node_id),
                   cookies=jar).json()
        return response

    @staticmethod
    def __make_url():
        cluster_name = os.getenv('CLUSTER_NAME').replace("_", "-")
        staging_url = "{}-crdb-admin.doorcrawl-int.com".format(cluster_name)
        prod_url = "{}-crdb-admin.doordash-int.com".format(cluster_name)
        if os.getenv('DEPLOYMENT_ENV') == "staging":
            return staging_url
        return prod_url
