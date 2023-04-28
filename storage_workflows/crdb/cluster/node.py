from storage_workflows.crdb.api_gateway.crdb_api_gateway import CrdbApiGateway

class Node:

    @staticmethod
    def get_nodes():
        session = CrdbApiGateway.login()
        return list(map(lambda node: Node(node), CrdbApiGateway.list_nodes(session)['nodes']))

    def __init__(self, api_response):
        self.api_response = api_response

    @property
    def ip_address(self):
        return self.api_response['address']['address_field']