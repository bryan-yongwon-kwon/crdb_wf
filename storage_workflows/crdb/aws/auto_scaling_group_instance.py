from functools import cached_property
from storage_workflows.crdb.api_gateway.ec2_gateway import Ec2Gateway
from storage_workflows.crdb.aws.ec2_instance import Ec2Instance

class AutoScalingGroupInstance:

    def __init__(self, api_response):
        self._api_response = api_response

    def in_service(self):
        return self._api_response['LifecycleState'] == "InService"
    
    @cached_property
    def instance_id(self):
        return self._api_response['InstanceId']
    