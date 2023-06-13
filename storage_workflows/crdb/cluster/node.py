from storage_workflows.crdb.api_gateway.crdb_api_gateway import CrdbApiGateway

class Node:

    @staticmethod
    def get_nodes():
        session = CrdbApiGateway.login()
        return list(map(lambda node: Node(node), CrdbApiGateway.list_nodes(session)['nodes']))

    def __init__(self, api_response):
        self.api_response = api_response

    @property
    def id(self):
        return self.api_response['node_id']
    
    @property
    def major_version(self):
        return self.api_response['ServerVersion']['major']

    @property
    def ip_address(self):
        return str(self.api_response['address']['address_field']).split(":")[0]
    
    @property
    def started_at(self):
        return self.api_response['started_at']
    
    @property
    def sql_conns(self):
        return int(self.api_response['metrics']['sql.conns'])
    
    def reload(self):
        self.api_response = list(filter(lambda node: node.id == self.id, Node.get_nodes()))[0].api_response