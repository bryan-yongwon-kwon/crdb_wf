import psycopg2
from storage_workflows.crdb.authentication.crdb_credential import CrdbCredential


class CrdbConnection:
    _HOST_SUFFIX = "-crdb.us-west-2.aws.ddnw.net"
    _PORT = "26257"
    _ROOT = "root"
    _SSL_MODE = "require"

    def __init__(
            self, 
            cluster_name: str,
            ca_cert:CrdbCredential, 
            public_cert:CrdbCredential, 
            private_key:CrdbCredential,
            db_name:str = "defaultdb"):
        self._cluster_name = cluster_name
        try:
            self._connection = psycopg2.connect(
                dbname=db_name,
                port=self._PORT,
                user=self._ROOT,
                host=cluster_name.replace('_', '-') + self._HOST_SUFFIX,
                sslmode=self._SSL_MODE,
                sslrootcert=ca_cert.get_credential_path(),
                sslcert=public_cert.get_credential_path(),
                sslkey=private_key.get_credential_path()
            )
        except Exception as error:
            print(error)
            raise

    def __del__(self):
        self.close()

    def close(self):
        self.connection.close()

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

    