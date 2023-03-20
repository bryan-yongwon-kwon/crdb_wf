import boto3
from storage_workflows.crdb.aws.account_type import AccountType

PROD_PROFILE_NAME = "okta-prod-storage-admin" 
STAGING_PROFILE_NAME = "okta-staging-storage-admin"

class AwsBase:
    PAGINATOR_MAX_RESULT_PER_PAGE = 100    

    @staticmethod
    def get_aws_client(account_type: AccountType, service_name:str, region:str):
        # Assume prod/staging IAM role place holder
        session = boto3.Session(
                profile_name=STAGING_PROFILE_NAME if account_type == AccountType.STAGING else PROD_PROFILE_NAME)
        return session.client(service_name, region_name=region) 