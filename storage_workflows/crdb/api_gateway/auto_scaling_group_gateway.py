import os
import time
from storage_workflows.crdb.factory.aws_session_factory import AwsSessionFactory
from storage_workflows.logging.logger import Logger

logger = Logger()

class AutoScalingGroupGateway:

    PAGINATOR_MAX_RESULT_PER_PAGE = 100

    @staticmethod
    def describe_auto_scaling_groups(filters=[], next_token='') -> list:
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
            logger.info('Auto Scaling group capacity updated successfully.')
        else:
            logger.error('Failed to update Auto Scaling group capacity.')
            logger.error('Error message:', response['ResponseMetadata']['HTTPHeaders']['x-amzn-requestid'])

    @staticmethod
    def detach_instance_from_autoscaling_group(instance_ids, autoscaling_group_name):
        auto_scaling_group_aws_client = AwsSessionFactory.auto_scaling()
        try:
            # Detach the instance from the Auto Scaling group
            auto_scaling_group_aws_client.detach_instances(
                InstanceIds=instance_ids,
                AutoScalingGroupName=autoscaling_group_name,
                ShouldDecrementDesiredCapacity=True
            )

            print(f"Instances have been removed from Auto Scaling group {autoscaling_group_name}.")

        except ClientError as e:
            error_message = e.response['Error']['Message']
            logger.error(
                f"Failed to remove instances from Auto Scaling group {autoscaling_group_name}: {error_message}")

    @staticmethod
    def enter_instances_into_standby(asg_name, instance_ids):
        auto_scaling_group_aws_client = AwsSessionFactory.auto_scaling()
        response = auto_scaling_group_aws_client.enter_standby(
            AutoScalingGroupName=asg_name,
            InstanceIds=instance_ids,
            ShouldDecrementDesiredCapacity=True
        )

        # Check the HTTP status code of the response
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            activities = response['Activities']
            AutoScalingGroupGateway.wait_for_activity_completion(activities, auto_scaling_group_aws_client)
        else:
            logger.error("Error: Failed to put instances into standby mode.")

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
            logger.error("Error: Failed to exit instances from standby mode.")

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
