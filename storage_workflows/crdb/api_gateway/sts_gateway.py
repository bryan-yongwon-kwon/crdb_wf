import os
from storage_workflows.crdb.factory.aws_session_factory import AwsSessionFactory

class StsGateway:

    @staticmethod
    def assume_role():
        deployment_env = os.getenv('DEPLOYMENT_ENV')
        role_arn = os.getenv('PROD_IAM_ROLE') if deployment_env == "prod" else os.getenv('STAGING_IAM_ROLE')
        sts_aws_client = AwsSessionFactory.sts()
        return sts_aws_client.assume_role(RoleArn="arn:aws:iam::611706558220:role/sso_storage_admin", RoleSessionName="prod")
