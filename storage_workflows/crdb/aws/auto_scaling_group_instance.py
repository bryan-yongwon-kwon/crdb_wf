from functools import cached_property

class AutoScalingGroupInstance:

    def __init__(self, api_response):
        self._api_response = api_response

    def in_service(self):
        return self._api_response['LifecycleState'] == "InService"

    def is_healthy(self):
        return self._api_response['HealthStatus'] == "Healthy"

    @cached_property
    def instance_id(self):
        return self._api_response['InstanceId']

    @property
    def health_status(self):
        return self._api_response['HealthStatus']

    @cached_property
    def availability_zone(self):
        return self._api_response['AvailabilityZone']

    @property
    def launch_time(self):
        return self._api_response['LaunchTime']