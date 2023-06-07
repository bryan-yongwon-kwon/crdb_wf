from __future__ import annotations
from storage_workflows.crdb.api_gateway.auto_scaling_group_gateway import AutoScalingGroupGateway
from storage_workflows.crdb.aws.auto_scaling_group_instance import AutoScalingGroupInstance
import os


class AutoScalingGroup:

    @staticmethod
    def find_all_auto_scaling_groups(filters: list) -> list:
        return list(map(lambda auto_scaling_group: AutoScalingGroup(auto_scaling_group),
                        AutoScalingGroupGateway.describe_auto_scaling_groups(filters)))
    
    @staticmethod
    def find_auto_scaling_group_by_cluster_name(cluster_name) -> AutoScalingGroup:
        filter = {
            'Name': 'tag:crdb_cluster_name',
            'Values': [
                cluster_name + "_" + os.getenv('DEPLOYMENT_ENV'),
            ]
        }
        return AutoScalingGroup.find_all_auto_scaling_groups([filter])[0]

    def __init__(self, api_response):
        self._api_response = api_response

    @property
    def instances(self) -> list:
        return list(map(lambda instance: AutoScalingGroupInstance(instance), self._api_response['Instances']))

    @property
    def capacity(self):
        return self._api_response['DesiredCapacity']

    @property
    def name(self):
        return self._api_response['AutoScalingGroupName']

    def instances_not_in_service_exist(self):
        return any(map(lambda instance: not instance.in_service(), self.instances))
