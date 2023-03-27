import boto3
from functools import cache


class AwsSessionFactory:
    
    @staticmethod
    @cache
    def auto_scaling():
        return boto3.client('autoscaling')
    
    @staticmethod
    @cache
    def secret_manager():
        return boto3.client("secretsmanager")
    
    @staticmethod
    @cache
    def sts():
        return boto3.client('sts')