from __future__ import annotations
from storage_workflows.crdb.api_gateway.auto_scaling_group_gateway import AutoScalingGroupGateway
from storage_workflows.crdb.aws.auto_scaling_group_instance import AutoScalingGroupInstance
from storage_workflows.crdb.aws.ec2_instance import Ec2Instance
from storage_workflows.crdb.models.node import Node
from storage_workflows.logging.logger import Logger
import os
import time

logger = Logger()

class AutoScalingGroup:

    @staticmethod
    def find_all_auto_scaling_groups(filters: list) -> list:
        return list(map(lambda auto_scaling_group: AutoScalingGroup(auto_scaling_group),
                        AutoScalingGroupGateway.describe_auto_scaling_groups(filters)))
    
    @staticmethod
    def find_auto_scaling_group_by_cluster_name(cluster_name) -> AutoScalingGroup:
        filter = AutoScalingGroup.build_filter_by_cluster_name(cluster_name)
        return AutoScalingGroup.find_all_auto_scaling_groups([filter])[0]
    
    @staticmethod
    def build_filter_by_cluster_name(cluster_name: str):
        return {
                    'Name': 'tag:crdb_cluster_name',
                    'Values': [
                        cluster_name + "_" + os.getenv('DEPLOYMENT_ENV'),
                    ]
                }

    def __init__(self, api_response):
        self._api_response = api_response

    @property
    def instances(self) -> list[AutoScalingGroupInstance]:
        return list(map(lambda instance: AutoScalingGroupInstance(instance), self._api_response['Instances']))

    @property
    def capacity(self):
        return self._api_response['DesiredCapacity']

    @property
    def name(self):
        return self._api_response['AutoScalingGroupName']

    def reload(self, cluster_name:str):
        self._api_response = AutoScalingGroupGateway.describe_auto_scaling_groups([AutoScalingGroup.build_filter_by_cluster_name(cluster_name)])[0]

    def instances_not_in_service_exist(self):
        return any(map(lambda instance: not instance.in_service(), self.instances))
    
    def add_ec2_instances(self, desired_capacity):
        asg_instances = self.instances

        if desired_capacity == self.capacity:
            logger.warning("Expected Desired capacity same as existing desired capacity.")
            return

        initial_actual_capacity = len(asg_instances)  # sum of all instances irrespective of their state
        old_instance_ids = set()
        # Retrieve the existing instance IDs
        for instance in asg_instances:
            old_instance_ids.add(instance.instance_id)

        AutoScalingGroupGateway.update_auto_scaling_group_capacity(self.name, desired_capacity)
        # Wait for the new instances to be added to the Auto Scaling group
        while True:
            asg_instances = AutoScalingGroupGateway.describe_auto_scaling_groups_by_name(self.name)[0]["Instances"]
            actual_capacity = len(asg_instances)
            new_instance_ids = set()  # Store new instance IDs
            # Retrieve the instance IDs of the newly added instances
            for instance in asg_instances:
                if instance["InstanceId"] not in old_instance_ids and instance["LifecycleState"] == "InService":
                    new_instance_ids.add(instance["InstanceId"])
            # Check if all new instances are found
            if len(new_instance_ids) == actual_capacity - initial_actual_capacity and actual_capacity != initial_actual_capacity:
                logger.info("All new instances are ready.")
                break
            # Wait before checking again
            time.sleep(10)

        return list(new_instance_ids)

    def check_equal_az_distribution_in_asg(self):
        az_count = {}
        for instance in self.instances:
            az = instance._api_response['AvailabilityZone']
            if az in az_count:
                az_count[az] += 1
            else:
                az_count[az] = 1

        max_instance_count = max(az_count.values())
        min_instance_count = min(az_count.values())

        return max_instance_count == min_instance_count and len(az_count) == 3
