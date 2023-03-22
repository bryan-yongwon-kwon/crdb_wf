from storage_workflows.crdb.api_gateway.auto_scaling_group_gateway import AutoScalingGroupGateway
from storage_workflows.crdb.aws.auto_scaling_group_instance import AutoScalingGroupInstance

class AutoScalingGroup:

    @staticmethod
    def find_all_auto_scaling_groups(auto_scaling_group_aws_client, filters: list) -> list:
        return list(map(lambda auto_scaling_group: AutoScalingGroup(auto_scaling_group),
                        AutoScalingGroupGateway.describe_auto_scaling_groups(auto_scaling_group_aws_client, filters)))

    def __init__(self, api_response: list):
        self._api_response = api_response

    def instances_not_in_service_exist(self):
        instances = list(map(lambda instance: AutoScalingGroupInstance(instance), self._api_response['Instances']))
        for instance in instances:
            if not instance.in_service():
                return True
        return False