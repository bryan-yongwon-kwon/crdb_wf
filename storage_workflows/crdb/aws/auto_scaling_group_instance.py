from functools import cached_property

class AutoScalingGroupInstance:

    def __init__(self, api_response):
        self._api_response = api_response

    def in_service(self):
        return self._api_response['LifecycleState'] == "InService"
    
    @cached_property
    def instance_id(self):
        return self._api_response['InstanceId']

    @property
    def health_status(self):
        return self._api_response['HealthStatus']
