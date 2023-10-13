import boto3
import os
import time
from botocore.exceptions import ClientError


class AwsSessionFactory:
    _cache = {}
    _cache_expiry = {}
    CACHE_DURATION = 3500  # Less than an hour to be safe

    @staticmethod
    def create_client(service_name):
        current_time = time.time()

        # Check if client exists in cache and hasn't expired
        if service_name in AwsSessionFactory._cache and \
                current_time < AwsSessionFactory._cache_expiry.get(service_name, 0):
            return AwsSessionFactory._cache[service_name]

        try:
            client = boto3.client(service_name,
                                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                                  aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
                                  region_name=os.getenv('REGION'))

            # Cache the newly created client and set its expiry time
            AwsSessionFactory._cache[service_name] = client
            AwsSessionFactory._cache_expiry[service_name] = current_time + AwsSessionFactory.CACHE_DURATION

            return client

        except ClientError as e:
            if e.response['Error']['Code'] == 'ExpiredToken':
                AwsSessionFactory.refresh_token()
                return AwsSessionFactory.create_client(service_name)
            else:
                raise e

    @staticmethod
    def refresh_token():
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
