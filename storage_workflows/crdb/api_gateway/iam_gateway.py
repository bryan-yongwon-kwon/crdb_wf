from storage_workflows.crdb.factory.aws_session_factory import AwsSessionFactory


class IamGateway:

    @staticmethod
    def get_account_alias():
        """
        Get AWS account alias

        """
        sts_aws_client = AwsSessionFactory.iam()
        aws_account_alias = sts_aws_client.list_account_aliases()
        return aws_account_alias['AccountAliases'][0] if aws_account_alias['AccountAliases'] else None
