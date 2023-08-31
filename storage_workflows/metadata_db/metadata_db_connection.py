import psycopg2
import os
import stat
from storage_workflows.logging.logger import Logger

logger = Logger()

class MetadataDBConnection:
    @staticmethod
    def write_to_file(dir_path, file_name, file_content):
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        file_path = os.path.join(dir_path, file_name)
        file = open(file_path, "w")
        file.write(file_content)
        file.close()
        os.chmod(file_path, stat.S_IREAD|stat.S_IWRITE)

    @staticmethod
    def get_connection_args(database_name, user_name):
        dir_path = os.getenv('CRDB_CERTS_DIR_PATH_PREFIX') + "/" + "storage-metadata" + "/"
        ca_cert = os.getenv('ROOT-CA')
        client_cert = os.getenv('CLIENT-CERT')
        client_key = os.getenv('CLIENT-KEY')
        client_cert_filename = "client."+user_name+".crt"
        client_key_filename = "client."+user_name+".key"
        MetadataDBConnection.write_to_file(dir_path, "ca.crt", ca_cert)
        MetadataDBConnection.write_to_file(dir_path, client_cert_filename, client_cert)
        MetadataDBConnection.write_to_file(dir_path, client_key_filename, client_key)

        args = {
            'database' : database_name,
            'port' : "26257",
            'user' : user_name,
            'host' : "storage-metadata-crdb.us-west-2.aws.ddnw.net",
            'sslmode' : "require",
            'sslrootcert' : dir_path + "ca.crt",
            'sslcert' : dir_path + "client.storage_metadata_app_20230509.crt",
            'sslkey' : dir_path + "client.storage_metadata_app_20230509.key"
        }
        return args
