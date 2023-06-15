from functools import cached_property
from storage_workflows.crdb.api_gateway.ec2_gateway import Ec2Gateway

class AutoScalingGroupInstance:

    def __init__(self, api_response):
        self._api_response = api_response

    def in_service(self):
        return self._api_response['LifecycleState'] == "InService"
    
    def terminate(self):
        response = Ec2Gateway.terminate_instances([self.instance_id])
        print(response)
        return response
    
    @cached_property
    def instance_id(self):
        return self._api_response['InstanceId']
    
    @cached_property
    def launch_time(self):
        filters = [{
            'Name': 'instance-id',
            'Values': [self.instance_id]
        }]
        return Ec2Gateway.describe_ec2_instances(filters)[0]['LaunchTime']
    