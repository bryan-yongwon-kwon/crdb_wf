import os
from storage_workflows.crdb.factory.aws_session_factory import AwsSessionFactory


class StsGateway:

    @staticmethod
    def assume_role():
        deployment_env = os.getenv('DEPLOYMENT_ENV')
        role_arn = os.getenv('PROD_IAM_ROLE') if deployment_env == "prod" else os.getenv('STAGING_IAM_ROLE')
        sts_aws_client = AwsSessionFactory.sts()
        return sts_aws_client.assume_role(RoleArn=role_arn, RoleSessionName=deployment_env)

    @staticmethod
    def get_account_alias():
        deployment_env = os.getenv('DEPLOYMENT_ENV')
        sts_aws_client = AwsSessionFactory.iam()
        aws_account_alias = sts_aws_client.list_account_aliases()
        return aws_account_alias['AccountAliases'][0] if aws_account_alias['AccountAliases'] else None