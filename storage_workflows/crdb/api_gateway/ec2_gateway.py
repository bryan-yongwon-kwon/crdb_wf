from storage_workflows.crdb.factory.aws_session_factory import AwsSessionFactory

class Ec2Gateway:

    PAGINATOR_MAX_RESULT_PER_PAGE = 100

    @staticmethod
    def describe_ec2_instances(filters=[], next_token=''):
        ec2_aws_client = AwsSessionFactory.ec2()
        if not next_token:
            response = ec2_aws_client.describe_instances(Filters=filters, 
                                                         MaxResults=Ec2Gateway.PAGINATOR_MAX_RESULT_PER_PAGE, 
                                                         DryRun=False)
        else:
            response = ec2_aws_client.describe_instances(Filters=filters, 
                                                         MaxResults=Ec2Gateway.PAGINATOR_MAX_RESULT_PER_PAGE, 
                                                         DryRun=False,
                                                         NextToken=next_token)
        instances = response['Reservations'][0]['Instances']
        if 'NextToken' in response:
            instances.extend(Ec2Gateway.describe_ec2_instances(filters, response['NextToken']))
        return instances
    
    @staticmethod
    def terminate_instances(instances:list[str]):
        ec2_aws_client = AwsSessionFactory.ec2()
        return ec2_aws_client.terminate_instances(InstanceIds=instances, DryRun=False)
