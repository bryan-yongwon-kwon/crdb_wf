from storage_workflows.crdb.factory.aws_session_factory import AwsSessionFactory
from storage_workflows.logging.logger import Logger

logger = Logger()

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
        instances = list(map(lambda reservation: reservation['Instances'][0], response['Reservations']))
        if 'NextToken' in response:
            instances.extend(Ec2Gateway.describe_ec2_instances(filters, response['NextToken']))
        return instances
    
    @staticmethod
    def terminate_instances(instances:list[str]):
        ec2_aws_client = AwsSessionFactory.ec2()
        return ec2_aws_client.terminate_instances(InstanceIds=instances, DryRun=False)

    @staticmethod
    def find_ec2_instances_with_tag(filters=[]):
        ec2_aws_client = AwsSessionFactory.ec2()
        # Create a paginator for the describe_instances method
        paginator = ec2_aws_client.get_paginator('describe_instances')
        page_iterator = paginator.paginate(Filters=filters)

        all_instances = []

        for page in page_iterator:
            for reservation in page['Reservations']:
                for instance in reservation['Instances']:
                    all_instances.append(instance)

        # for instance in all_instances:
        #    logger.info(f"Found EC2 instance with ID: {instance['InstanceId']}")

        return all_instances

