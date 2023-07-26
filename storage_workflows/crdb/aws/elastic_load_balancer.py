from __future__ import annotations
from functools import cached_property
from storage_workflows.crdb.api_gateway.elastic_load_balancer_gateway import ElasticLoadBalancerGateway
from storage_workflows.logging.logger import Logger

logger = Logger()

class ElasticLoadBalancer:

    @staticmethod
    def find_elastic_load_balancers(names:list) -> list:
        return list(map(lambda load_balancer: ElasticLoadBalancer(load_balancer),
                        ElasticLoadBalancerGateway.describe_load_balancers(names)))
    
    @staticmethod
    def find_elastic_load_balancer_by_cluster_name(cluster_name:str) -> ElasticLoadBalancer:
        etl_load_balancer_name = (cluster_name.replace("_", "-") + "-crdb-etl")[:32]
        load_balancers = ElasticLoadBalancer.find_elastic_load_balancers([etl_load_balancer_name])
        if not load_balancers:
            logger.error("Mode not enabled. ETL load balancer doesn't exist.")
            raise Exception('No ETL load balancer found!')
        return load_balancers[0]
    
    def __init__(self, api_response):
        self.api_response = api_response

    @cached_property
    def load_balancer_name(self):
        return self.api_response['LoadBalancerName']
    
    @property
    def instances(self) -> list:
        return self.api_response['Instances']
    
    def reload(self):
        self.api_response = list(filter(lambda load_balancer: load_balancer['LoadBalancerName'] == self.load_balancer_name, 
                                        ElasticLoadBalancerGateway.describe_load_balancers([self.load_balancer_name])))[0]
        
    def register_instances(self, instances:list):
        ElasticLoadBalancerGateway.register_instances_with_load_balancer(self.load_balancer_name, instances)
        self.reload()

    def deregister_instances(self, instances:list):
        ElasticLoadBalancerGateway.deregister_instances_from_load_balancer(self.load_balancer_name, instances)
        self.reload()