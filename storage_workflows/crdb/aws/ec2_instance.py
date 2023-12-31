from __future__ import annotations
import time
import os
from functools import cached_property
from storage_workflows.crdb.api_gateway.ec2_gateway import Ec2Gateway
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
from storage_workflows.crdb.models.node import Node
from storage_workflows.logging.logger import Logger

logger = Logger()

class Ec2Instance:

    @staticmethod
    def find_ec2_instance(instance_id:str) -> Ec2Instance:
        filters = [{
            'Name': 'instance-id',
            'Values': [instance_id]
        }]
        return Ec2Instance(Ec2Gateway.describe_ec2_instances(filters)[0])
    
    @staticmethod
    def find_ec2_instances_by_cluster_tag(cluster_name:str) -> list[Ec2Instance]:
        filters = [{
            'Name': 'tag:crdb_cluster_name',
            'Values': [
                cluster_name + "_" + os.getenv('DEPLOYMENT_ENV'),
            ]
        }]
        # return list(map(lambda response: Ec2Instance(response), Ec2Gateway.describe_ec2_instances(filters)))
        return list(map(lambda response: Ec2Instance(response), Ec2Gateway.find_ec2_instances_with_tag(filters)))

    def __init__(self, api_response):
        self._api_response = api_response

    @property
    def instance_id(self):
        return self._api_response['InstanceId']
    
    @property
    def launch_time(self):
        return self._api_response['LaunchTime']
    
    # should be one of these: 'pending'|'running'|'shutting-down'|'terminated'|'stopping'|'stopped'
    @property
    def state(self):
        return self._api_response['State']['Name']
    
    @property
    def private_ip_address(self):
        return self._api_response['PrivateIpAddress']
    
    @cached_property
    def crdb_node(self) -> Node:
        return list(filter(lambda node: node.ip_address == self.private_ip_address, Node.get_nodes()))[0]
    
    def reload(self):
        filters = [{
            'Name': 'instance-id',
            'Values': [self.instance_id]
        }]
        self._api_response = Ec2Gateway.describe_ec2_instances(filters)[0]
    
    def terminate_instance(self):
        logger.info("Terminating instance {}...".format(self.instance_id))
        Ec2Gateway.terminate_instances([self.instance_id])
        while self.state != 'terminated':
            logger.info("Current state is {}".format(self.state))
            logger.info("sleeping 30s...")
            time.sleep(30)
            self.reload()
        logger.info("Instance {} terminated.".format(self.instance_id))

    

