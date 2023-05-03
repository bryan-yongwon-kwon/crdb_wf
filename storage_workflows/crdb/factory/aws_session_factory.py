import boto3
import os
from functools import cache


class AwsSessionFactory:
    
    @staticmethod
    @cache
    def auto_scaling():
        return boto3.client('autoscaling',
                            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                            aws_session_token=os.getenv('AWS_SESSION_TOKEN'))
    
    @staticmethod
    @cache
    def secret_manager():
        return boto3.client("secretsmanager",
                            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                            aws_session_token=os.getenv('AWS_SESSION_TOKEN'))
    
    @staticmethod
    @cache
    def sts():
        return boto3.client('sts',
                            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                            aws_session_token=os.getenv('AWS_SESSION_TOKEN'))
    
    @staticmethod
    @cache
    def ec2():
        return boto3.client('ec2',
                            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                            aws_session_token=os.getenv('AWS_SESSION_TOKEN'))