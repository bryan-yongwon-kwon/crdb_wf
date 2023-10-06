import os
from requests import get, post, cookies
from storage_workflows.logging.logger import Logger

logger = Logger()


class CrdbApiGateway:

    @staticmethod
    def login():
        response = post("https://{}/api/v2/login/?username=root&password={}".format(CrdbApiGateway.__make_url(), os.getenv('ROOT_PASSWORD')))
        # DEBUG
        logger.info(f"response for CrdbApiGateway.login: {response}")
        return response.json()["session"]
    
    @staticmethod
    def list_nodes(session:str, limit=200, offset=0):
        response = get("https://{}/api/v2/nodes/?limit={}&offset={}".format(CrdbApiGateway.__make_url(), limit, offset),
            headers={"X-Cockroach-API-Session": session}).json()
        # DEBUG
        logger.info(f"response for CrdbApiGateway.list_nodes: {response}")
        return response
    
    @staticmethod
    def get_node_details_from_endpoint(session:str, node_id:str):
        jar = cookies.RequestsCookieJar()
        jar.set(name='session', value=session, path='/')
        response = get("https://{}/_status/nodes/{}".format(CrdbApiGateway.__make_url(), node_id),
                   cookies=jar).json()
        # DEBUG
        logger.info(f"response for CrdbApiGateway.get_node_details_from_endpoint: {response}")
        return response

    @staticmethod
    def __make_url():
        cluster_name = os.getenv('CLUSTER_NAME').replace("_", "-")
        staging_url = "{}-crdb-admin.doorcrawl-int.com".format(cluster_name)
        prod_url = "{}-crdb-admin.doordash-int.com".format(cluster_name)
        # DEBUG
        logger.info(f"response for CrdbApiGateway.__make_url: {prod_url}")
        if os.getenv('DEPLOYMENT_ENV') == "staging":
            return staging_url
        return prod_url
