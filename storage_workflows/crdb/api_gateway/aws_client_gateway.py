import boto3
from storage_workflows.crdb.aws.deployment_env import DeploymentEnv


class AwsClientGateway:

    PROD_PROFILE_NAME = "okta-prod-storage-admin" 
    STAGING_PROFILE_NAME = "okta-staging-storage-admin"
    REGION = "us-west-2"

    SECRET_MANAGER_SERVICE_NAME = "secretsmanager"
    AUTO_SCALING_GROUP_SERVICE_NAME = "autoscaling"

    @staticmethod
    def get_aws_client(deployment_env: DeploymentEnv, service_name: str):
        session = boto3.Session(
                profile_name=AwsClientGateway.STAGING_PROFILE_NAME if deployment_env == DeploymentEnv.STAGING else AwsClientGateway.PROD_PROFILE_NAME)
        return session.client(service_name, region_name=AwsClientGateway.REGION) 