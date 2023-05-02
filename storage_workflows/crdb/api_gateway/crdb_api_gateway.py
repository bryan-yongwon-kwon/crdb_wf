import os
from requests import get, post

class CrdbApiGateway:

    @staticmethod
    def login():
        test_response = post("https://{}".format(CrdbApiGateway.__make_url()))
        print("Test connect UI1: {}".format(test_response))
        print("Test connect UI2: {}".format(test_response.content))
        print("Test connect UI3: {}".format(test_response.text))
        response = post("https://{}/api/v2/login/?username=root&password={}".format(CrdbApiGateway.__make_url(), os.getenv('ROOT_PASSWORD')))
        return response.json()["session"]
    
    @staticmethod
    def list_nodes(session:str, limit=200, offset=0):
        return get("{}/api/v2/nodes/?limit={}}&offset={}".format(CrdbApiGateway.__make_url(), limit, offset),
                       headers={"X-Cockroach-API-Session":session}).json()

    @staticmethod
    def __make_url():
        cluster_name = os.getenv('CLUSTER_NAME').replace("_", "-")
        if os.getenv('DEPLOYMENT_ENV') == "staging":
            return "{}-crdb-admin.doorcrawl-int.com".format(cluster_name)
        return  "{}-crdb-admin.doordash-int.com".format(cluster_name)