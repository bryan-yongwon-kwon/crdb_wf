from __future__ import annotations
from storage_workflows.crdb.api_gateway.auto_scaling_group_gateway import AutoScalingGroupGateway
from storage_workflows.crdb.aws.auto_scaling_group_instance import AutoScalingGroupInstance
from storage_workflows.logging.logger import Logger
import os
import time
from botocore.exceptions import ClientError
from storage_workflows.crdb.factory.aws_session_factory import AwsSessionFactory

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
    
    @staticmethod
    def build_filter_by_crdb_tag():
        return {
                    'Name': 'tag-key',
                    'Values': ['crdb_cluster_name',]
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

    @property
    def availability_zone(self):
        return self._api_response['AvailabilityZone']

    @property
    def launch_template(self):
        return self._api_response.get('LaunchTemplate', {})

    @property
    def instance_type(self):
        launch_template_id = None

        # Check if launch_template property contains the necessary data
        if self.launch_template and 'LaunchTemplateId' in self.launch_template:
            launch_template_id = self.launch_template['LaunchTemplateId']

        instance_type_value = AutoScalingGroupGateway._get_instance_type_from_launch_configuration_or_template(
            launch_template_id=launch_template_id
        )

        logger.info(f"Instance type derived for ASG {self.name}: {instance_type_value}")
        return instance_type_value

    @property
    def current_instances(self):
        return AutoScalingGroupGateway._get_current_asg_instances(self.name)

    @property
    def launch_template_id(self):
        return self._api_response.get('LaunchTemplate', {}).get('Id')

    def reload(self, cluster_name:str):
        self._api_response = AutoScalingGroupGateway.describe_auto_scaling_groups([AutoScalingGroup.build_filter_by_cluster_name(cluster_name)])[0]

    def instances_not_in_service_exist(self):
        return any(map(lambda instance: not instance.in_service(), self.instances))

    def add_ec2_instances(self, desired_capacity, autoscale=False):
        asg_instances = self.instances
        if desired_capacity == self.capacity and not autoscale:
            logger.warning("Expected Desired capacity same as existing desired capacity.")
            return

        initial_actual_capacity = len(asg_instances)  # sum of all instances irrespective of their state
        old_instance_ids = set()
        # Retrieve the existing instance IDs
        for instance in asg_instances:
            old_instance_ids.add(instance.instance_id)

        # Use a dry run to ensure there's enough capacity for the desired instance type
        if not self.dry_run_check_instance_availability(desired_capacity - self.capacity):
            raise Exception(f"Dry run failed to validate the availability {self.instance_type}")

        # Update ASG capacity
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

    # STORAGE-7583:  remove instances if scaling down
    def get_instances_to_terminate(self, num_of_instances_to_terminate):
        """
        Get a list of instance IDs to terminate based on the desired number of instances to terminate.
        Ensure an equal distribution of nodes across availability zones.
        :param instances_to_terminate: Number of instances to terminate
        :return: List of instance IDs to terminate
        """
        if num_of_instances_to_terminate <= 0:
            return []

        instance_ids_to_terminate = []
        instances = self.instances

        az_count = {"us-west-2a": 0, "us-west-2b": 0, "us-west-2c": 0}
        instances.sort(key=lambda instance: instance.launch_time)

        for instance in instances:
            az = instance.availability_zone
            if az in az_count:
                az_count[az] += 1

        az_list = list(az_count.keys())
        num_azs = len(az_list)
        target_termination_count = num_of_instances_to_terminate // num_azs
        remainder = num_of_instances_to_terminate % num_azs

        for az in az_list:
            if remainder > 0:
                termination_count = target_termination_count + 1
                remainder -= 1
            else:
                termination_count = target_termination_count

            for instance in instances:
                if instance.availability_zone == az and termination_count > 0:
                    instance_ids_to_terminate.append(instance.instance_id)
                    az_count[az] -= 1
                    num_of_instances_to_terminate -= 1
                    termination_count -= 1

                if num_of_instances_to_terminate == 0:
                    break

        return instance_ids_to_terminate

    def get_current_az_distribution(self):
        az_count = {"us-west-2a": 0, "us-west-2b": 0, "us-west-2c": 0}
        for instance in self.instances:
            az = instance.availability_zone
            if az in az_count:
                az_count[az] += 1
        return az_count

    def get_image_id_from_launch_template(self):
        launch_template_id = self.launch_template.get('LaunchTemplateId', None)

        if not launch_template_id:
            logger.error("Launch template ID is missing or None.")
            return None

        ec2_client = AwsSessionFactory.ec2()

        try:
            response = ec2_client.describe_launch_template_versions(
                LaunchTemplateId=launch_template_id,
                Versions=['$Latest']
            )
            logger.info(f"ec2_client.describe_launch_template_versions response: {response}")
            if 'LaunchTemplateVersions' in response and len(response['LaunchTemplateVersions']) > 0:
                return response['LaunchTemplateVersions'][0]['LaunchTemplateData'].get('ImageId')
            else:
                logger.error(f"No launch template version found with ID: {launch_template_id}")
                return None
        except Exception as e:
            logger.error(f"Error fetching launch template version with ID: {launch_template_id}. Error: {str(e)}")
            return None

    def dry_run_check_instance_availability(self, desired_increase):
        """Perform a dry run to check if enough instances of the desired type are available."""
        ec2_client = AwsSessionFactory.ec2()
        image_id = self.get_image_id_from_launch_template()

        if not image_id:
            logger.error("Failed to retrieve ImageId from launch template.")
            return False

        logger.info(f"dry run self.instance_type: {self.instance_type}")
        logger.info(f"dry run self.capacity: {self.capacity}")
        logger.info(f"dry run image_id: {image_id}")
        logger.info(f"dry run desired_increase: {desired_increase}")

        # Number of retries every 10 minutes for 12 hours is 72 times
        MAX_RETRIES = 72
        RETRY_INTERVAL = 10 * 60  # 10 minutes in seconds

        for _ in range(MAX_RETRIES):
            try:
                ec2_client.run_instances(
                    DryRun=True,
                    InstanceType=self.instance_type,
                    MaxCount=self.capacity + desired_increase,
                    MinCount=self.capacity + desired_increase,
                    ImageId=image_id
                )
            except ClientError as e:
                if 'DryRunOperation' in str(e):
                    # successful dry run with DryRun=True will result in this exception
                    return True
                else:
                    # Log the exception and wait before retrying
                    logger.error(
                        f"Dry run failed with exception: {str(e)}. Retrying in {RETRY_INTERVAL / 60} minutes...")
                    time.sleep(RETRY_INTERVAL)
                    continue
            else:
                # If no exception occurred, break out of the loop
                break

        # If we've exhausted all our retries, raise an exception
        raise Exception("Max retries reached. Failed to validate instance availability through dry run.")

