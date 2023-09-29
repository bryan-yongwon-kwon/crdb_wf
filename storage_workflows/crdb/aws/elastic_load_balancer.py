from __future__ import annotations
from functools import cached_property
from storage_workflows.crdb.api_gateway.elastic_load_balancer_gateway import ElasticLoadBalancerGateway
from storage_workflows.logging.logger import Logger
from botocore.exceptions import ClientError

logger = Logger()


class ElasticLoadBalancer:

    @property
    def instances(self) -> list:
        return self.api_response['Instances']

    @staticmethod
    def find_elastic_load_balancers(names:list) -> list:
        return list(map(lambda load_balancer: ElasticLoadBalancer(load_balancer),
                        ElasticLoadBalancerGateway.describe_load_balancers(names)))
    
    @staticmethod
    def find_elastic_load_balancer_by_cluster_name(cluster_name:str) -> ElasticLoadBalancer | None:
        try:
            etl_load_balancer_name = (cluster_name.replace("_", "-") + "-crdb-etl")[:32]
            load_balancers = ElasticLoadBalancer.find_elastic_load_balancers([etl_load_balancer_name])
            if not load_balancers:
                logger.error("Mode not enabled. ETL load balancer doesn't exist.")
                raise Exception('No ETL load balancer found!')
            logger.info(f"Using load balancer name: {load_balancers}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'LoadBalancerNotFound':
                return None
            else:
                raise e
        return load_balancers[0]

    def __init__(self, api_response):
        self.api_response = api_response


    @cached_property
    def load_balancer_name(self):
        return self.api_response['LoadBalancerName']

    def reload(self):
        self.api_response = list(filter(lambda load_balancer: load_balancer['LoadBalancerName'] == self.load_balancer_name, 
                                        ElasticLoadBalancerGateway.describe_load_balancers([self.load_balancer_name])))[0]
        
    def register_instances(self, instances:list):
        try:
            if ElasticLoadBalancerGateway.register_instances_with_load_balancer(self.load_balancer_name, instances) is not None:
                self.reload()
                return True
        except ClientError as e:
            logger.error(f"e: {e.response}")
            # Handle other possible exceptions or re-raise
            logger.error("Unhandled client exception occurred while calling elb gateway.")
            return False

    def deregister_instances(self, instances:list):
        ElasticLoadBalancerGateway.deregister_instances_from_load_balancer(self.load_balancer_name, instances)
        self.reload()

    def __repr__(self):
        return f"<ElasticLoadBalancer(name={self.load_balancer_name}, instances={len(self.instances)})>"
