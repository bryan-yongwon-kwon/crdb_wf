class AutoScalingGroupInstance:

    def __init__(self, api_response):
        self._api_response = api_response

    def in_service(self):
        return self._api_response['LifecycleState'] == "InService"
    