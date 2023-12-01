import os
import time
from storage_workflows.crdb.factory.aws_session_factory import AwsSessionFactory
from storage_workflows.logging.logger import Logger
from botocore.exceptions import ClientError


logger = Logger()

class AutoScalingGroupGateway:

    PAGINATOR_MAX_RESULT_PER_PAGE = 100

    @staticmethod
    def describe_auto_scaling_groups(filters=[], next_token='') -> list:
        try:
            auto_scaling_group_aws_client = AwsSessionFactory.auto_scaling()
            if not next_token:
                response = auto_scaling_group_aws_client.describe_auto_scaling_groups(Filters=filters, MaxRecords=AutoScalingGroupGateway.PAGINATOR_MAX_RESULT_PER_PAGE)
            else:
                response = auto_scaling_group_aws_client.describe_auto_scaling_groups(Filters=filters,
                                                                   MaxRecords=AutoScalingGroupGateway.PAGINATOR_MAX_RESULT_PER_PAGE,
                                                                   NextToken=next_token)
            if 'NextToken' in response:
                response['AutoScalingGroups'].extend(
                    AutoScalingGroupGateway.describe_auto_scaling_groups(filters, response['NextToken']))
            return response['AutoScalingGroups']
        except ClientError as ce:
            logger.error(f"AWS ClientError while describing auto scaling groups with filters {filters}: {str(ce)}")
            raise ce
        except Exception as e:
            logger.error(f"General error while describing auto scaling groups with filters {filters}: {str(e)}")
            raise e

    @staticmethod
    def describe_auto_scaling_groups_by_name(asg_name):
        auto_scaling_group_aws_client = AwsSessionFactory.auto_scaling()
        response = auto_scaling_group_aws_client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[asg_name]
        )

        return response['AutoScalingGroups']

    @staticmethod
    def update_auto_scaling_group_capacity(auto_scaling_group_name, desired_capacity):
        auto_scaling_group_aws_client = AwsSessionFactory.auto_scaling()
        response = auto_scaling_group_aws_client.update_auto_scaling_group(
            AutoScalingGroupName=auto_scaling_group_name,
            DesiredCapacity=desired_capacity
        )

        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info(f'Auto Scaling group capacity successfully updated to {desired_capacity}.')
        else:
            logger.error(f"Error message: {response['ResponseMetadata']['HTTPHeaders']['x-amzn-requestid']}")
            raise Exception('Failed to update Auto Scaling group capacity.')

    @staticmethod
    def detach_instance_from_autoscaling_group(instance_ids, autoscaling_group_name):
        auto_scaling_group_aws_client = AwsSessionFactory.auto_scaling()

        try:
            # Detach the instance from the Auto Scaling group
            response = auto_scaling_group_aws_client.detach_instances(
                InstanceIds=instance_ids,
                AutoScalingGroupName=autoscaling_group_name,
                ShouldDecrementDesiredCapacity=True
            )

            # Check the HTTP status code of the response
            if response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
                activities = response.get('Activities', [])
                if not activities:
                    logger.error("Error: Activities list is empty after detaching instance.")
                    return
                AutoScalingGroupGateway.wait_for_activity_completion(activities, auto_scaling_group_aws_client)
            else:
                error_msg = response.get('ResponseMetadata', {}).get('HTTPHeaders', {}).get('x-amzn-requestid',
                                                                                            'Unknown Error')
                logger.error(f"Error: Failed to detach instances from autoscaling group. AWS Error: {error_msg}")
                raise Exception(f"Error: Failed to detach instances from autoscaling group. AWS Error: {error_msg}")

        except Exception as e:
            logger.error(f"An exception occurred: {str(e)}")
            raise e

    @staticmethod
    def enter_instances_into_standby(asg_name, instance_ids):
        if not instance_ids or not isinstance(instance_ids, (list, tuple)):
            logger.error("Error: Invalid instance_ids provided.")
            raise ValueError("Invalid instance_ids provided. Expected a non-empty list or tuple.")

        asg_client = AwsSessionFactory.auto_scaling()

        try:
            response = asg_client.enter_standby(
                AutoScalingGroupName=asg_name,
                InstanceIds=instance_ids,
                ShouldDecrementDesiredCapacity=True
            )
            # Check the HTTP status code of the response
            if response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
                activities = response.get('Activities', [])
                if not activities:
                    logger.error("Error: Activities list is empty after entering standby.")
                    return
                AutoScalingGroupGateway.wait_for_activity_completion(activities, asg_client)
            else:
                error_msg = response.get('ResponseMetadata', {}).get('HTTPHeaders', {}).get('x-amzn-requestid',
                                                                                            'Unknown Error')
                logger.error(f"Error: Failed to put instances into standby mode. AWS Error: {error_msg}")
                raise Exception(f"Error: Failed to put instances into standby mode. AWS Error: {error_msg}")

        except Exception as e:
            logger.error(f"An exception occurred: {str(e)}")
            raise e

    @staticmethod
    def exit_instances_from_standby(asg_name, instance_ids):
        auto_scaling_group_aws_client = AwsSessionFactory.auto_scaling()
        response = auto_scaling_group_aws_client.exit_standby(
            AutoScalingGroupName=asg_name,
            InstanceIds=instance_ids
        )

        # Check the HTTP status code of the response
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            activities = response['Activities']
            AutoScalingGroupGateway.wait_for_activity_completion(activities, auto_scaling_group_aws_client)
        else:
            raise Exception("Error: Failed to exit instances from standby mode.")

    @staticmethod
    def wait_for_activity_completion(activities, auto_scaling_group_aws_client):
        for activity in activities:
            activity_id = activity['ActivityId']
            description = activity['Description']
            status_code = activity['StatusCode']

            logger.info(f"Activity ID: {activity_id}")
            logger.info(f"Description: {description}")

            # Wait and check the status code until it becomes "Successful"
            while status_code != 'Successful':
                time.sleep(10)  # Wait for 10 seconds

                response = auto_scaling_group_aws_client.describe_scaling_activities(
                    ActivityIds=[activity_id]
                )

                if response['Activities'][0]['StatusCode'] == 'Failed':
                    logger.error("Error: Failed Activity")
                    break

                status_code = response['Activities'][0]['StatusCode']

            logger.info(f"Status Code: {status_code}")
        return

    @staticmethod
    def _get_current_asg_instances(asg_name):
        auto_scaling_client = AwsSessionFactory.auto_scaling()
        response = auto_scaling_client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[asg_name]
        )
        instances = response['AutoScalingGroups'][0]['Instances']
        return [instance['InstanceId'] for instance in instances]

    def _get_instance_type_from_launch_configuration_or_template(launch_template_id=None,
                                                                 launch_configuration_name=None):
        if launch_template_id:
            ec2_client = AwsSessionFactory.ec2()
            response = ec2_client.describe_launch_template_versions(
                LaunchTemplateId=launch_template_id,
                Versions=['$Latest']
            )

            if 'LaunchTemplateVersions' in response and len(response['LaunchTemplateVersions']) > 0:
                return response['LaunchTemplateVersions'][0]['LaunchTemplateData'].get('InstanceType')
            else:
                logger.error(f"No launch template version found with ID: {launch_template_id}")
                return None

        elif launch_configuration_name:
            auto_scaling_client = AwsSessionFactory.auto_scaling()
            response = auto_scaling_client.describe_launch_configurations(
                LaunchConfigurationNames=[launch_configuration_name]
            )

            if 'LaunchConfigurations' in response and len(response['LaunchConfigurations']) > 0:
                return response['LaunchConfigurations'][0].get('InstanceType')
            else:
                logger.error(f"No launch configuration found with name: {launch_configuration_name}")
                return None

        else:
            logger.error("Both LaunchTemplateId and LaunchConfigurationName are missing.")
            return None

    @staticmethod
    def get_auto_scaling_group_capacity(auto_scaling_group_name):
        auto_scaling_group_aws_client = AwsSessionFactory.auto_scaling()
        response = auto_scaling_group_aws_client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[auto_scaling_group_name]
        )
        asg_details = response['AutoScalingGroups'][0]
        return asg_details['MinSize'], asg_details['MaxSize'], asg_details['DesiredCapacity']

    @staticmethod
    def set_auto_scaling_group_capacity(auto_scaling_group_name, min_size, max_size, desired_capacity):
        auto_scaling_group_aws_client = AwsSessionFactory.auto_scaling()
        auto_scaling_group_aws_client.update_auto_scaling_group(
            AutoScalingGroupName=auto_scaling_group_name,
            MinSize=min_size,
            MaxSize=max_size,
            DesiredCapacity=desired_capacity
        )
        logger.info(f'Updated {auto_scaling_group_name} ASG with MinSize: {min_size}, MaxSize: {max_size}, DesiredCapacity: {desired_capacity}.')

    @staticmethod
    def decrease_min_capacity(auto_scaling_group_name):
        min_size, max_size, desired_capacity = AutoScalingGroupGateway.get_auto_scaling_group_capacity(
            auto_scaling_group_name)

        # Only decrease if min_capacity is same as desired_capacity and more than 2
        if min_size == desired_capacity and min_size > 2:
            AutoScalingGroupGateway.set_auto_scaling_group_capacity(auto_scaling_group_name, min_size - 1, max_size,
                                                                    desired_capacity)
            logger.info(f'Decreased minimum capacity of {auto_scaling_group_name} by 1.')
        else:
            logger.info(
                f'Cannot decrease minimum capacity of {auto_scaling_group_name}. Current MinSize: {min_size}, DesiredCapacity: {desired_capacity}.')

    @staticmethod
    def increase_min_capacity(auto_scaling_group_name):
        min_size, max_size, desired_capacity = AutoScalingGroupGateway.get_auto_scaling_group_capacity(auto_scaling_group_name)
        AutoScalingGroupGateway.set_auto_scaling_group_capacity(auto_scaling_group_name, min_size + 1, max_size, desired_capacity)
        logger.info(f'Increased minimum capacity of {auto_scaling_group_name} by 1.')
