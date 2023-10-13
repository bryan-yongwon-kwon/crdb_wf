import boto3
import os
from botocore.exceptions import ClientError


class AwsSessionFactory:

    @staticmethod
    def create_client(service_name):
        try:
            client = boto3.client(service_name,
                                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                                  aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
                                  region_name=os.getenv('REGION'))
            # Just a dummy call to check token validity.
            if service_name == 's3':
                client.list_buckets()
            return client
        except ClientError as e:
            if e.response['Error']['Code'] == 'ExpiredToken':
                AwsSessionFactory.refresh_token()
                return AwsSessionFactory.create_client(service_name)
            else:
                raise e

    @staticmethod
    def refresh_token():
        # Assuming you have a mechanism to refresh your token and set environment variables.
        # Here's a simple example using sts. Adjust according to your setup.
        sts_client = boto3.client('sts',
                                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                                  region_name=os.getenv('REGION'))

        response = sts_client.get_session_token()
        os.environ['AWS_ACCESS_KEY_ID'] = response['Credentials']['AccessKeyId']
        os.environ['AWS_SECRET_ACCESS_KEY'] = response['Credentials']['SecretAccessKey']
        os.environ['AWS_SESSION_TOKEN'] = response['Credentials']['SessionToken']

    @staticmethod
    def auto_scaling():
        return AwsSessionFactory.create_client('autoscaling')

    @staticmethod
    def secret_manager():
        return AwsSessionFactory.create_client('secretsmanager')

    @staticmethod
    def sts():
        return AwsSessionFactory.create_client('sts')

    @staticmethod
    def ec2():
        return AwsSessionFactory.create_client('ec2')

    @staticmethod
    def elb():
        return AwsSessionFactory.create_client('elb')

    @staticmethod
    def iam():
        return AwsSessionFactory.create_client('iam')

    @staticmethod
    def ebs():
        return AwsSessionFactory.create_client('ebs')

    @staticmethod
    def s3():
        return AwsSessionFactory.create_client('s3')

    @staticmethod
    def elbv2():
        return AwsSessionFactory.create_client('elbv2')
