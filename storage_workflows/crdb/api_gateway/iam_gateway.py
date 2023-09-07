from storage_workflows.crdb.factory.aws_session_factory import AwsSessionFactory
import logging
import boto3
import os
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
iam = boto3.resource('iam')

class IamGateway:

    @staticmethod
    def get_account_alias():
        """
        Get AWS account alias

        """
        sts_aws_client = AwsSessionFactory.iam()
        aws_account_alias = sts_aws_client.list_account_aliases()
        return aws_account_alias['AccountAliases'][0] if aws_account_alias['AccountAliases'] else None

    @staticmethod
    def list_policies(deployment_env):
        """
        Lists inline policies for a role.

        :param deployment_env: name of env
        """
        #role_arn = os.getenv('PROD_IAM_ROLE') if deployment_env == "prod" else os.getenv('STAGING_IAM_ROLE')
        role_name = "storage-workflows"
        try:
            role = iam.Role(role_name)
            for policy in role.policies.all():
                logger.info("Got inline policy %s.", policy.name)
        except ClientError:
            logger.exception("Couldn't list inline policies for %s.", role_name)
            raise