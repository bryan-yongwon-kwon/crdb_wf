from storage_workflows.crdb.connect.cred_type import CredType
from storage_workflows.crdb.aws.deployment_env import DeploymentEnv
from storage_workflows.crdb.api_gateway.secret_manager_gateway import SecretManagerGateway
from storage_workflows.crdb.aws.secret_value import SecretValue
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.aws.secret import Secret
from storage_workflows.crdb.aws.secret_value import SecretValue


class CrdbConnectGateway:
    CRDB_SUFFIX = "-crdb"
    CERTS_DIR_PATH_PREFIX = "/Users/aochen/Projects/storage-workflows/crdb"

    @staticmethod
    def get_crdb_secret(secret_manager_aws_client, deployment_env:DeploymentEnv, cred_type:CredType, cluster_name:str, client:str="") -> SecretValue:
        cluster_name_with_suffix = cluster_name + CrdbConnectGateway.CRDB_SUFFIX
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
                    deployment_env.value,
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
        secret_list = Secret.find_all_secrets(secret_manager_aws_client, secret_filters)
        list_count = len(secret_list)
        assert list_count == 1, "Should get exact 1 {} for {}. Got {} now.".format(cred_type.value, cluster_name, list_count)
        return SecretValue(SecretManagerGateway.find_secret(secret_manager_aws_client, secret_list[0].secret_arn()))
    
    @staticmethod
    def connect_crdb_cluster(secret_manager_aws_client, deployment_env:DeploymentEnv, cluster_name:str, client:str=""):
        ca_cert = CrdbConnectGateway.get_crdb_secret(secret_manager_aws_client, deployment_env, CredType.CA_CERT_CRED_TYPE, cluster_name)
        public_cert = CrdbConnectGateway.get_crdb_secret(secret_manager_aws_client, deployment_env, CredType.PUBLIC_CERT_CRED_TYPE, cluster_name, client)
        private_cert = CrdbConnectGateway.get_crdb_secret(secret_manager_aws_client, deployment_env, CredType.PRIVATE_KEY_CRED_TYPE, cluster_name, client)
        dir_path = CrdbConnectGateway.CERTS_DIR_PATH_PREFIX + "/" + cluster_name + "/"
        ca_cert.write_to_file(dir_path, "ca.crt")
        public_cert.write_to_file(dir_path, "client."+client+".crt")
        private_cert.write_to_file(dir_path, "client."+client+".key")
        return CrdbConnection(cluster_name, dir_path)