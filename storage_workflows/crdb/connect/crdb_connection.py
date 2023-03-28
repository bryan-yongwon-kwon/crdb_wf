import os
import psycopg2
from storage_workflows.crdb.connect.cred_type import CredType
from storage_workflows.crdb.aws.secret import Secret
from storage_workflows.crdb.aws.secret_value import SecretValue
from storage_workflows.crdb.api_gateway.secret_manager_gateway import SecretManagerGateway


class CrdbConnection:

    @staticmethod
    def get_crdb_connection_secret(cred_type:CredType, cluster_name:str, client:str="") -> SecretValue:
        cluster_name_with_suffix = cluster_name + "-crdb"
        secret_filters = {'tag-key':['crdb_cluster_name', 'cred-type', 'environment'],
                          'tag-value':[cred_type.value, os.getenv('DEPLOYMENT_ENV'), cluster_name_with_suffix], 
                          'description':['!DEPRECATED']}
        if client:
            secret_filters['tag-key'].append('client')
            secret_filters['tag-value'].append(client)
        secret_list = Secret.find_all_secrets(transform_filters(secret_filters))
        return SecretValue(SecretManagerGateway.find_secret(secret_list[0].arn))
    
    @staticmethod
    def get_crdb_connection(cluster_name:str):
        crdb_client = os.getenv('CRDB_CLIENT')
        ca_cert = CrdbConnection.get_crdb_connection_secret(CredType.CA_CERT_CRED_TYPE, cluster_name)
        public_cert = CrdbConnection.get_crdb_connection_secret(CredType.PUBLIC_CERT_CRED_TYPE, cluster_name, crdb_client)
        private_cert = CrdbConnection.get_crdb_connection_secret(CredType.PRIVATE_KEY_CRED_TYPE, cluster_name, crdb_client)
        dir_path = os.getenv('CRDB_CERTS_DIR_PATH_PREFIX') + "/" + cluster_name + "/"
        ca_cert.write_to_file(dir_path, os.getenv('CRDB_CA_CERT_FILE_NAME'))
        public_cert.write_to_file(dir_path, os.getenv('CRDB_PUBLIC_CERT_FILE_NAME'))
        private_cert.write_to_file(dir_path, os.getenv('CRDB_PRIVATE_KEY_FILE_NAME'))
        return CrdbConnection(cluster_name, dir_path)

    def __init__(
            self, 
            cluster_name: str,
            db_name:str = "defaultdb",):
        self._cluster_name = cluster_name
        self._credential_dir_path = os.getenv('CRDB_CERTS_DIR_PATH_PREFIX') + "/" + cluster_name + "/"
        self._db_name = db_name
        self._client = os.getenv('CRDB_CLIENT')

    def connect(self):
        try:
            self._connection = psycopg2.connect(
                dbname=self._db_name,
                port=os.getenv('CRDB_PORT'),
                user=self._client,
                host=self._cluster_name.replace('_', '-') + os.getenv('CRDB_HOST_SUFFIX'),
                sslmode=os.getenv('CRDB_CONNECTION_SSL_MODE'),
                sslrootcert=self._credential_dir_path + os.getenv('CRDB_CA_CERT_FILE_NAME'),
                sslcert=self._credential_dir_path + os.getenv('CRDB_PUBLIC_CERT_FILE_NAME'),
                sslkey=self._credential_dir_path + os.getenv('CRDB_PRIVATE_KEY_FILE_NAME')
            )
        except Exception as error:
            print(error)
            raise

    def close(self):
        if self._connection:
            self._connection.close() 

    def execute_sql(self, sql:str, need_commit: bool):
        cursor = self._connection.cursor()
        try:
            cursor.execute(sql)
            if need_commit:
                self._connection.commit()
        except Exception as error:
            print(error)
            raise
        return cursor.fetchall()

    
def transform_filters(filters):
    transformed_filters = []
    for filter_key in filters.keys():
        for filter_value in filters[filter_key]:
            transformed_filters.append({
                'Key': filter_key,
                'Values': [
                    filter_value,
                ]
            })
    return transformed_filters