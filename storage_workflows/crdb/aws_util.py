import boto3
from common_enums import CredType
from common_enums import Env

CRDB_SUFFIX = "-crdb"
SECRET_MAX_RESULT = 100
SECRET_MANAGER = "secretsmanager"
REGION = "us-west-2"
ROOT = "root"
STAGING_PROFILE_NAME = "okta-staging-storage-admin"
PROD_PROFILE_NAME = "okta-prod-storage-admin"

def get_aws_client_local(env: Env, service_name:str):
    # Assume prod/staging IAM role place holder
    session = boto3.Session(profile_name=STAGING_PROFILE_NAME if env == Env.STAGING else PROD_PROFILE_NAME)
    return session.client(service_name, region_name=REGION)

def get_secret(aws_client, cluster_name:str, env:Env, cred_type:CredType, client: str="") -> str:
    cluster_name_with_suffix = cluster_name + CRDB_SUFFIX
    secret_filters = [
            {
                'Key': 'tag-key',
                'Values': [
                    'crdb_cluster_name',
                ]
            },
            {
                'Key': 'tag-key',
                'Values': [
                    'cred-type',
                ]
            },
            {
                'Key': 'tag-key',
                'Values': [
                    'environment',
                ]
            },
            {
                'Key': 'tag-value',
                'Values': [
                    cred_type.value,
                ]
            },
            {
                'Key': 'tag-value',
                'Values': [
                    env.value,
                ]
            },
            {
                'Key': 'tag-value',
                'Values': [
                    cluster_name_with_suffix,
                ]
            },
            {
                'Key': 'description',
                'Values': [
                    '!DEPRECATED',
                ]
            }
        ]
    if client:
        secret_filters.extend([
            {
                'Key': 'tag-key',
                'Values': [
                    'client',
                ]
            },
            {
                'Key': 'tag-value',
                'Values': [
                    client,
                ]
            },
        ])
    secret_list = aws_client.list_secrets(
        IncludePlannedDeletion=False,
        MaxResults=SECRET_MAX_RESULT,
        Filters=secret_filters
    )['SecretList']
    result_count = len(secret_list)
    assert result_count == 1, "Should get exact 1 {} for {}. Got {} now.".format(cred_type.value, cluster_name, result_count)
    return aws_client.get_secret_value(SecretId=secret_list[0]['ARN'])['SecretString']



def get_crdb_creds(env:Env, cluster_name:str) -> dict:
    secrets_aws_client = get_aws_client_local(env, SECRET_MANAGER)
    private_key = get_secret(secrets_aws_client, cluster_name, env, CredType.PRIVATE_KEY_CRED_TYPE, ROOT)
    public_cert = get_secret(secrets_aws_client, cluster_name, env, CredType.PUBLIC_CERT_CRED_TYPE, ROOT)
    ca_cert = get_secret(secrets_aws_client, cluster_name, env, CredType.CA_CERT_CRED_TYPE)
    return {
        CredType.PRIVATE_KEY_CRED_TYPE.value: private_key,
        CredType.PUBLIC_CERT_CRED_TYPE.value: public_cert,
        CredType.CA_CERT_CRED_TYPE.value: ca_cert
    }
