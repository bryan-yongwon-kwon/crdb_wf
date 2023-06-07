from storage_workflows.crdb.factory.aws_session_factory import AwsSessionFactory
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
    def update_auto_scaling_group_capacity(auto_scaling_group_name, desired_capacity):
        auto_scaling_group_aws_client = AwsSessionFactory.auto_scaling()
        response = auto_scaling_group_aws_client.update_auto_scaling_group(
            AutoScalingGroupName=auto_scaling_group_name,
            DesiredCapacity=desired_capacity
        )

        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print('Auto Scaling group capacity updated successfully.')
        else:
            print('Failed to update Auto Scaling group capacity.')
            print('Error message:', response['ResponseMetadata']['HTTPHeaders']['x-amzn-requestid'])
    

    @staticmethod
    def get_auto_scaling_group_capacity(auto_scaling_group_name):
        auto_scaling_group_aws_client = AwsSessionFactory.auto_scaling()
        response = auto_scaling_group_aws_client.describe_auto_scaling_groups(AutoScalingGroupNames=[auto_scaling_group_name])
        print(response)
        desired_capacity = response['AutoScalingGroups'][0]['DesiredCapacity']
        print('Desired capacity:', desired_capacity)
        return desired_capacity

    @staticmethod
    def remove_instance_from_autoscaling_group(instance_id, autoscaling_group_name):
        auto_scaling_group_aws_client = AwsSessionFactory.auto_scaling()
        try:
            # Detach the instance from the Auto Scaling group
            auto_scaling_group_aws_client.detach_instances(
                InstanceIds=[instance_id],
                AutoScalingGroupName=autoscaling_group_name,
                ShouldDecrementDesiredCapacity=True
            )

            print(f"Instance {instance_id} has been removed from Auto Scaling group {autoscaling_group_name}.")

        except ClientError as e:
            error_message = e.response['Error']['Message']
            print(
                f"Failed to remove instance {instance_id} from Auto Scaling group {autoscaling_group_name}: {error_message}")

