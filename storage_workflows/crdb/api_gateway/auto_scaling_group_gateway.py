class AutoScalingGroupGateway:

    PAGINATOR_MAX_RESULT_PER_PAGE = 100

    @staticmethod
    def describe_auto_scaling_groups(auto_scaling_group_aws_client, filters=[], next_token='') -> list:
        if not next_token:
            response = auto_scaling_group_aws_client.describe_auto_scaling_groups(Filters=filters, MaxRecords=AutoScalingGroupGateway.PAGINATOR_MAX_RESULT_PER_PAGE)
        else:
            response = auto_scaling_group_aws_client.describe_auto_scaling_groups(Filters=filters, 
                                                               MaxRecords=AutoScalingGroupGateway.PAGINATOR_MAX_RESULT_PER_PAGE,
                                                               NextToken=next_token)
        if 'NextToken' in response:
            response['AutoScalingGroups'].extend(
                AutoScalingGroupGateway.describe_auto_scaling_groups(auto_scaling_group_aws_client, filters, response['NextToken']))
        return response['AutoScalingGroups']