import boto3
from account_type import AccountType


class AwsBase:
    PAGINATOR_MAX_RESULT_PER_PAGE = 100

    _PROD_PROFILE_NAME = "okta-prod-storage-admin" 
    _STAGING_PROFILE_NAME = "okta-staging-storage-admin"

    def get_aws_client_local(self, account_type: AccountType, service_name:str, region:str):
        # Assume prod/staging IAM role place holder
        session = boto3.Session(
                profile_name=self._STAGING_PROFILE_NAME if account_type == AccountType.STAGING else self._PROD_PROFILE_NAME)
        return session.client(service_name, region_name=region) 