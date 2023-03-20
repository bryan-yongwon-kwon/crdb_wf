from aws_base import AwsBase
from storage_workflows.crdb.authentication.cred_type import CredType
from storage_workflows.crdb.aws.account_type import AccountType



class SecretManager(AwsBase):
    _CRDB_SUFFIX = "-crdb"
    _DEFAULT_CLIENT = "root"
    _SERVICE_NAME = "secretsmanager"

    def __init__(self, account_type:AccountType, cluster_name:str):
        self._aws_client = AwsBase.get_aws_client_local(account_type, self._SERVICE_NAME)
        self._account_type = account_type
        self._cluster_name = cluster_name

    def _get_crdb_secret(self, cred_type:CredType, client: str="") -> str:
        cluster_name_with_suffix = self._cluster_name + self._CRDB_SUFFIX
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
                        self._account_type.value,
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
        secret_list = self._aws_client.list_secrets(
            IncludePlannedDeletion=False,
            MaxResults=AwsBase.PAGINATOR_MAX_RESULT_PER_PAGE,
            Filters=secret_filters
        )['SecretList']
        result_count = len(secret_list)
        assert result_count == 1, "Should get exact 1 {} for {}. Got {} now.".format(cred_type.value, self._cluster_name, result_count)
        return self._aws_client.get_secret_value(SecretId=secret_list[0]['ARN'])['SecretString']
    
    def get_crdb_ca_cert(self) -> str:
        return self._get_crdb_secret(CredType.CA_CERT_CRED_TYPE)
    
    def get_crdb_public_cert(self) -> str:
        return self._get_crdb_secret(CredType.PUBLIC_CERT_CRED_TYPE, self._DEFAULT_CLIENT)
    
    def get_crdb_private_key(self) -> str:
        return self._get_crdb_secret(CredType.PRIVATE_KEY_CRED_TYPE, self._DEFAULT_CLIENT)