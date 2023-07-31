from __future__ import annotations
from storage_workflows.crdb.api_gateway.auto_scaling_group_gateway import AutoScalingGroupGateway
from storage_workflows.crdb.aws.auto_scaling_group_instance import AutoScalingGroupInstance
from storage_workflows.crdb.aws.ec2_instance import Ec2Instance
from storage_workflows.crdb.models.node import Node
from storage_workflows.logging.logger import Logger
import math
import os
import statistics
import time

logger = Logger()

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

    @staticmethod
    def wait_for_hydration(asg_name):
        asg_instances = AutoScalingGroupGateway.describe_auto_scaling_groups_by_name(asg_name)[0]["Instances"]
        instance_ids = []
        for instance in asg_instances:
            instance_ids.append(instance["InstanceId"])
        nodes = list(map(lambda instance_id: Ec2Instance.find_ec2_instance(instance_id).crdb_node, instance_ids))
        logger.info("Checking nodes for hydration!")

        while True:
            nodes_replications_dict = {node: node.replicas for node in nodes}
            replications_values = list(nodes_replications_dict.values())
            avg_replications = statistics.mean(replications_values)
            outliers = [node for node, replications in nodes_replications_dict.items() if
                        abs(replications - avg_replications) / avg_replications > 0.1]
            if not any(outliers):
                logger.info("Hydration complete")
                break
            logger.info("Waiting for nodes to hydrate.")
            for outlier in outliers:
                logger.info(f"Node: {outlier.id} Replications: {nodes_replications_dict[outlier]}")
            time.sleep(60)
        return

    @staticmethod
    def add_ec2_instances(asg_name, desired_capacity):
        asg_instances = AutoScalingGroupGateway.describe_auto_scaling_groups_by_name(asg_name)[0]["Instances"]
        initial_capacity = len(asg_instances)
        old_instance_ids = set()
        # Retrieve the existing instance IDs
        for instance in asg_instances:
            old_instance_ids.add(instance["InstanceId"])

        AutoScalingGroupGateway.update_auto_scaling_group_capacity(asg_name, desired_capacity)
        # Wait for the new instances to be added to the Auto Scaling group
        while True:
            asg_instances = AutoScalingGroupGateway.describe_auto_scaling_groups_by_name(asg_name)[0]["Instances"]
            new_instance_ids = set()  # Store new instance IDs
            # Retrieve the instance IDs of the newly added instances
            for instance in asg_instances:
                if instance["InstanceId"] not in old_instance_ids and instance["LifecycleState"] == "InService":
                    new_instance_ids.add(instance["InstanceId"])
            # Check if all new instances are found
            if len(new_instance_ids) == desired_capacity - initial_capacity:
                logger.info("All new instances are ready.")
                break
            # Wait before checking again
            time.sleep(10)

        return list(new_instance_ids)

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

    def instances_not_in_service_exist(self):
        return any(map(lambda instance: not instance.in_service(), self.instances))
