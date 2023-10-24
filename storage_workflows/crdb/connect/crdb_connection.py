import os
import psycopg2
import psycopg2.pool as pool
from storage_workflows.crdb.connect.cred_type import CredType
from storage_workflows.crdb.aws.secret import Secret
from storage_workflows.crdb.aws.secret_value import SecretValue
from storage_workflows.crdb.api_gateway.secret_manager_gateway import SecretManagerGateway
from storage_workflows.logging.logger import Logger
from psycopg2 import OperationalError, InterfaceError, ProgrammingError

logger = Logger()


class CrdbConnection:

    @staticmethod
    def get_crdb_connection_secret(cred_type: CredType, cluster_name: str, client: str = "") -> SecretValue:
        cluster_name_with_suffix = cluster_name + "-crdb"
        cluster_name_with_hyphens = cluster_name.replace("_", "-") + "-crdb"  # Declare this here for broader scope

        secret_filters = {
            'tag-key': ['crdb_cluster_name', 'cred-type', 'environment'],
            'tag-value': [cred_type.value, os.getenv('DEPLOYMENT_ENV'), cluster_name_with_suffix],
            'description': ['!DEPRECATED']
        }

        if client:
            secret_filters['tag-key'].append('client')
            secret_filters['tag-value'].append(client)

        secret_list = Secret.find_all_secrets(transform_filters(secret_filters))

        # If secret_list is empty, use the cluster_name_with_hyphens to retry
        if client and 'client' not in secret_filters['tag-key']:
            secret_filters['tag-key'].append('client')
            secret_filters['tag-value'].append(client)

        # If secret_list is still empty after the retry, raise an error
        if not secret_list:
            raise ValueError(
                f"No secrets found for cluster_name: {cluster_name} or {cluster_name_with_hyphens} with cred_type: "
                f"{cred_type.value}.")

        find_secret_value = SecretValue(SecretManagerGateway.find_secret(secret_list[0].arn))
        return find_secret_value

    @staticmethod
    def get_crdb_connection(cluster_name: str, db_name: str = "defaultdb"):
        crdb_client = os.getenv('CRDB_CLIENT')
        ca_cert = CrdbConnection.get_crdb_connection_secret(CredType.CA_CERT_CRED_TYPE, cluster_name)
        public_cert = CrdbConnection.get_crdb_connection_secret(CredType.PUBLIC_CERT_CRED_TYPE, cluster_name,
                                                                crdb_client)
        private_cert = CrdbConnection.get_crdb_connection_secret(CredType.PRIVATE_KEY_CRED_TYPE, cluster_name,
                                                                 crdb_client)
        dir_path = os.getenv('CRDB_CERTS_DIR_PATH_PREFIX') + "/" + cluster_name + "/"
        ca_cert.write_to_file(dir_path, os.getenv('CRDB_CA_CERT_FILE_NAME'))
        public_cert.write_to_file(dir_path, os.getenv('CRDB_PUBLIC_CERT_FILE_NAME'))
        private_cert.write_to_file(dir_path, os.getenv('CRDB_PRIVATE_KEY_FILE_NAME'))
        return CrdbConnection(cluster_name, db_name)

    def __init__(self, cluster_name: str, db_name: str):
        self._connection = None
        self._cluster_name = cluster_name
        self._credential_dir_path = os.getenv('CRDB_CERTS_DIR_PATH_PREFIX') + "/" + cluster_name + "/"
        self._db_name = db_name
        self._client = os.getenv('CRDB_CLIENT')

    @property
    def connection(self):
        return self._connection

    def get_connection_pool(self, min_conn, max_conn):
        host_suffix = os.getenv('CRDB_PROD_HOST_SUFFIX') if os.getenv('DEPLOYMENT_ENV') == 'prod' else os.getenv(
            'CRDB_STAGING_HOST_SUFFIX')
        try:
            conn_pool = pool.ThreadedConnectionPool(min_conn,
                                                    max_conn,
                                                    dbname=self._db_name,
                                                    port=os.getenv('CRDB_PORT'),
                                                    user=self._client,
                                                    host=self._cluster_name.replace('_', '-') + host_suffix,
                                                    sslmode=os.getenv('CRDB_CONNECTION_SSL_MODE'),
                                                    sslrootcert=self._credential_dir_path + os.getenv(
                                                        'CRDB_CA_CERT_FILE_NAME'),
                                                    sslcert=self._credential_dir_path + os.getenv(
                                                        'CRDB_PUBLIC_CERT_FILE_NAME'),
                                                    sslkey=self._credential_dir_path + os.getenv(
                                                        'CRDB_PRIVATE_KEY_FILE_NAME'),
                                                    application_name='operator-service-argo-workflow')
            return conn_pool
        except Exception as error:
            logger.error(error)
            raise

    def connect(self):
        host_suffix = os.getenv('CRDB_PROD_HOST_SUFFIX') if os.getenv('DEPLOYMENT_ENV') == 'prod' else os.getenv(
            'CRDB_STAGING_HOST_SUFFIX')
        try:
            self._connection = psycopg2.connect(
                dbname=self._db_name,
                port=os.getenv('CRDB_PORT'),
                user=self._client,
                host=self._cluster_name.replace('_', '-') + host_suffix,
                sslmode=os.getenv('CRDB_CONNECTION_SSL_MODE'),
                sslrootcert=self._credential_dir_path + os.getenv('CRDB_CA_CERT_FILE_NAME'),
                sslcert=self._credential_dir_path + os.getenv('CRDB_PUBLIC_CERT_FILE_NAME'),
                sslkey=self._credential_dir_path + os.getenv('CRDB_PRIVATE_KEY_FILE_NAME'),
                application_name='operator-service-argo-workflow', # do we have an envar with the actual workflow name?
                connect_timeout=2  # Set the connection timeout to 2 seconds
            )
            # check for connection error
            if self._connection is None:
                raise ValueError("Connection is not established.")

        except (psycopg2.DatabaseError, ValueError) as error:
            logger.error(f"Error: {error}")

    def close(self):
        if self._connection:
            self._connection.close()

    def execute_sql(self, sql: str, need_commit: bool = False, need_fetchall: bool = True, need_fetchone: bool = False,
                    need_connection_close: bool = False, auto_commit: bool = False):
        try:
            if self._connection is None:
                raise ValueError("Connection is not established.")
            self._connection.autocommit = auto_commit
            cursor = self._connection.cursor()
            cursor.execute(sql)
            if need_commit:
                self._connection.commit()
        except Exception as error:
            logger.error(error)
            raise
        try:
            if need_fetchone:
                result = cursor.fetchone()
                return result
            if need_fetchall:
                result = cursor.fetchall()
                return result
        except OperationalError as oe:
            logger.error(f"Operational error: {oe}")
        except ProgrammingError as pe:
            logger.error(f"Programming error (e.g., table not found, syntax error): {pe}")
        except InterfaceError as ie:
            logger.error(f"Interface error (e.g., bad connection string): {ie}")
        finally:
            if self._connection and need_connection_close:
                self._connection.close()


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
