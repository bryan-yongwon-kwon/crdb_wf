import boto3


class IamGateway:

    @staticmethod
    def get_account_alias():
        sts_aws_client = boto3.client('iam')
        aws_account_alias = sts_aws_client.list_account_aliases()
        return aws_account_alias['AccountAliases'][0] if aws_account_alias['AccountAliases'] else None