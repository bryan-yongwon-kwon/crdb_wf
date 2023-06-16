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

    @cached_property
    def instance_id(self):
        return self._api_response['InstanceId']
    
    @cached_property
    def launch_time(self):
        return self._api_response['LaunchTime']
    
    # should be one of these: 'pending'|'running'|'shutting-down'|'terminated'|'stopping'|'stopped'
    @property
    def state(self):
        return self._api_response['State']['Name']
    
    def terminate_instance(self):
        print("Terminating instance {}...".format(self.instance_id))
        response = Ec2Gateway.terminate_instances([self.instance_id])
        while response['CurrentState']['Name'] != 'terminated':
            print("Current state is {}, previous state is {}".format(response['CurrentState']['Name'], response['PreviousState']['Name']))
            print("sleeping 30s...")
            time.sleep(30)
        print("Instance {} terminated.".format(self.instance_id))

