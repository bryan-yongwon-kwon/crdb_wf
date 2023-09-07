from storage_workflows.crdb.factory.aws_session_factory import AwsSessionFactory
import logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
iam = boto3.resource('iam')

class IamGateway:

    @staticmethod
    def get_account_alias():
        sts_aws_client = AwsSessionFactory.iam()
        aws_account_alias = sts_aws_client.list_account_aliases()
        return aws_account_alias['AccountAliases'][0] if aws_account_alias['AccountAliases'] else None

    @staticmethod
    def list_policies(role_name):
        """
        Lists inline policies for a role.

        :param role_name: The name of the role to query.
        """
        sts_aws_client = AwsSessionFactory.iam()
        try:
            role = iam.Role(role_name)
            for policy in role.policies.all():
                logger.info("Got inline policy %s.", policy.name)
        except ClientError:
            logger.exception("Couldn't list inline policies for %s.", role_name)
            raise