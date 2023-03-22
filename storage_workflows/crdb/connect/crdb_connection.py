import psycopg2


class CrdbConnection:
    
    HOST_SUFFIX = "-crdb.us-west-2.aws.ddnw.net"
    DEFAULT_DB = "defaultdb"
    PORT = "26257"
    ROOT = "root"
    SSL_MODE = "require"

    CA_CERT = "ca.crt"
    PUBLIC_CERT = "client.root.crt"
    PRIVATE_CERT = "client.root.key"

    def __init__(
            self, 
            cluster_name: str,
            credential_dir_path: str,
            db_name:str = DEFAULT_DB):
        try:
            self._connection = psycopg2.connect(
                dbname=db_name,
                port=self.PORT,
                user=self.ROOT,
                host=cluster_name.replace('_', '-') + self.HOST_SUFFIX,
                sslmode=self.SSL_MODE,
                sslrootcert=credential_dir_path + self.CA_CERT,
                sslcert=credential_dir_path + self.PUBLIC_CERT,
                sslkey=credential_dir_path + self.PRIVATE_CERT
            )
        except Exception as error:
            print(error)
            raise

    def __del__(self):
        self._connection.close()

    def close(self):
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

    