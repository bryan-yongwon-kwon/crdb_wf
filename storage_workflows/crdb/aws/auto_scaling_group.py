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
        return self._api_response['LaunchTemplate']

    @property
    def instance_type(self):
        launch_template_id = None
        launch_configuration_name = self.launch_configuration_name

        # Check if launch_template property contains the necessary data
        if self.launch_template and 'LaunchTemplateId' in self.launch_template:
            launch_template_id = self.launch_template['LaunchTemplateId']

        instance_type_value = AutoScalingGroupGateway._get_instance_type_from_launch_configuration_or_template(
            launch_template_id=launch_template_id,
            launch_configuration_name=launch_configuration_name
        )

        logger.info(f"Instance type derived for ASG {self.name}: {instance_type_value}")
        return instance_type_value

    @property
    def current_instances(self):
        return AutoScalingGroupGateway._get_current_asg_instances(self.name)

    @property
    def launch_configuration_name(self):
        lc_name = self._api_response.get('LaunchConfigurationName', None)
        logger.info(f"LaunchConfigurationName for ASG {self.name}: {lc_name}")
        return lc_name

    @property
    def launch_template_id(self):
        return self._api_response.get('LaunchTemplate', {}).get('Id')

    def reload(self, cluster_name:str):
        self._api_response = AutoScalingGroupGateway.describe_auto_scaling_groups([AutoScalingGroup.build_filter_by_cluster_name(cluster_name)])[0]

    def instances_not_in_service_exist(self):
        return any(map(lambda instance: not instance.in_service(), self.instances))

    def add_ec2_instances(self, desired_capacity, autoscale=False):
        asg_instances = AutoScalingGroupGateway._get_current_asg_instances(self.name)

        if desired_capacity == self.capacity and not autoscale:
            logger.warning("Expected Desired capacity same as existing desired capacity.")
            return

        old_instance_ids = set(asg_instances)

        # Check if instance type is available across AZs
        desired_increase = desired_capacity - self.capacity
        available_azs = self.get_az_with_available_instances(desired_increase)

        if not available_azs:
            logger.error("No instance type available in desired AZs.")
            return

        # Update ASG capacity
        AutoScalingGroupGateway.update_auto_scaling_group_capacity(self.name, desired_capacity)

        # This is just a brief sleep to allow AWS some time for instantiation.
        time.sleep(10)

        new_asg_instances = AutoScalingGroupGateway._get_current_asg_instances(self.name)
        new_instance_ids = set(new_asg_instances) - old_instance_ids

        if len(new_instance_ids) != desired_increase:
            logger.warning("Not all instances were properly instantiated.")

        # TODO: Additional steps to handle these new instances, if necessary.

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
        launch_template_id = self.launch_template_id

        if not launch_template_id:
            logger.error("Launch template ID is missing or None.")
            return None

        ec2_client = AwsSessionFactory.ec2()

        try:
            response = ec2_client.describe_launch_template_versions(
                LaunchTemplateId=launch_template_id,
                Versions=['$Latest']
            )

            if 'LaunchTemplateVersions' in response and len(response['LaunchTemplateVersions']) > 0:
                return response['LaunchTemplateVersions'][0]['LaunchTemplateData'].get('ImageId')
            else:
                logger.error(f"No launch template version found with ID: {launch_template_id}")
                return None
        except Exception as e:
            logger.error(f"Error fetching launch template version with ID: {launch_template_id}. Error: {str(e)}")
            return None

    def get_az_with_available_instances(self, desired_increase: int) -> list:
        az_count = self.get_current_az_distribution()
        ec2_client = AwsSessionFactory.ec2()
        available_azs = []
        image_id = self.get_image_id_from_launch_template()

        if not image_id:
            logger.error("Failed to retrieve ImageId from launch template.")
            return []

        for az, count in az_count.items():
            try:
                ec2_client.run_instances(
                    DryRun=True,
                    InstanceType=self.instance_type,
                    MaxCount=count + desired_increase,
                    MinCount=count + desired_increase,
                    Placement={'AvailabilityZone': az},
                    ImageId=image_id
                )
                available_azs.append(az)
            except ClientError as e:
                if "DryRunOperation" not in str(e):
                    logger.error(f"Failed to check instance availability in {az}: {e}")

        return available_azs
