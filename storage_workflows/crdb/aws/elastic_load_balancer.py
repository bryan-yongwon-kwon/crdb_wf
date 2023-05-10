from storage_workflows.crdb.api_gateway.elastic_load_balancer_gateway import ElasticLoadBalancerGateway
from functools import cached_property

class ElasticLoadBalancer:

    @staticmethod
    def find_elastic_load_balancers(names:list) -> list:
        return list(map(lambda load_balancer: ElasticLoadBalancer(load_balancer),
                        ElasticLoadBalancerGateway.describe_load_balancers(names)))
    
    def __init__(self, api_response):
        self.api_response = api_response

    @cached_property
    def load_balancer_name(self):
        return self.api_response['LoadBalancerName']
    
    @property
    def instances(self):
        return self.api_response['Instances']