import os
from requests import get, post, cookies

class CrdbApiGateway:

    @staticmethod
    def login():
        response = post("https://{}/api/v2/login/?username=root&password={}".format(CrdbApiGateway.__make_url(), os.getenv('ROOT_PASSWORD')))
        return response.json()["session"]
    
    @staticmethod
    def list_nodes(session:str, limit=200, offset=0):
        return get("https://{}/api/v2/nodes/?limit={}&offset={}".format(CrdbApiGateway.__make_url(), limit, offset),
                       headers={"X-Cockroach-API-Session":session}).json()
    
    @staticmethod
    def get_node_details_from_endpoint(session:str, node_id:str):
        jar = cookies.RequestsCookieJar()
        jar.set(name='session', value=session, path='/')
        return get("https://{}/_status/nodes/{}".format(CrdbApiGateway.__make_url(), node_id),
                   cookies=jar).json()

    @staticmethod
    def __make_url():
        cluster_name = os.getenv('CLUSTER_NAME').replace("_", "-")
        if os.getenv('DEPLOYMENT_ENV') == "staging":
            return "{}-crdb-admin.doorcrawl-int.com".format(cluster_name)
        return  "{}-crdb-admin.doordash-int.com".format(cluster_name)