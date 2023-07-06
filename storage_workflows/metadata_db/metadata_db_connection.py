import psycopg2
import os
import stat

class MetadataDBConnection:

    def __init__(self, database_name="crdb_workflows", user_name="storage_metadata_app_20230509"):
        self._user_name = user_name
        self._database_name = database_name

    def connect(self):
        dir_path = os.getenv('CRDB_CERTS_DIR_PATH_PREFIX') + "/" + "storage-metadata" + "/"
        ca_cert = os.getenv('ROOT-CA')
        client_cert = os.getenv('CLIENT-CERT')
        client_key = os.getenv('CLIENT-KEY')
        client_cert_filename = "client."+self._user_name+".crt"
        client_key_filename = "client."+self._user_name+".key"
        MetadataDBOperations.write_to_file(dir_path, "ca.crt", ca_cert)
        MetadataDBOperations.write_to_file(dir_path, client_cert_filename, client_cert)
        MetadataDBOperations.write_to_file(dir_path, client_key_filename, client_key)

        self._connection = psycopg2.connect(
            database=self._database_name,
            port= "26257",
            user=self._user_name,
            host= "storage-metadata-crdb.us-west-2.aws.ddnw.net",
            sslmode="require",
            sslrootcert=dir_path + "ca.crt",
            sslcert=dir_path + "client.storage_metadata_app_20230509.crt",
            sslkey=dir_path + "client.storage_metadata_app_20230509.key"
        )

    def execute_sql(self, sql: str, need_commit: bool = False, need_fetchall: bool = False, need_fetchone: bool = False):
        cursor = self._connection.cursor()
        try:
            cursor.execute(sql)
            if need_commit:
                self._connection.commit()
        except Exception as error:
            print(error)
            raise
        if need_fetchall:
            return cursor.fetchall()
        if need_fetchone:
            return cursor.fetchone()

    def close(self):
        if self._connection:
            self._connection.close()

    def write_to_file(dir_path, file_name, file_content):
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        file_path = os.path.join(dir_path, file_name)
        file = open(file_path, "w")
        file.write(file_content)
        file.close()
        os.chmod(file_path, stat.S_IREAD|stat.S_IWRITE)
