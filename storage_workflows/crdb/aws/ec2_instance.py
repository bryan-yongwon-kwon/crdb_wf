from __future__ import annotations
import time
from functools import cached_property
from storage_workflows.crdb.api_gateway.ec2_gateway import Ec2Gateway

class Ec2Instance:

    @staticmethod
    def find_ec2_instance(instance_id:str) -> Ec2Instance:
        filters = [{
            'Name': 'instance-id',
            'Values': [instance_id]
        }]
        return Ec2Instance(Ec2Gateway.describe_ec2_instances(filters)[0])

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
    
    def reload(self):
        filters = [{
            'Name': 'instance-id',
            'Values': [self.instance_id]
        }]
        self._api_response = Ec2Gateway.describe_ec2_instances(filters)[0]
    
    def terminate_instance(self):
        print("Terminating instance {}...".format(self.instance_id))
        Ec2Gateway.terminate_instances([self.instance_id])
        while self.state != 'terminated':
            print("Current state is {}".format(self.state))
            print("sleeping 30s...")
            time.sleep(30)
            self.reload()
        print("Instance {} terminated.".format(self.instance_id))

