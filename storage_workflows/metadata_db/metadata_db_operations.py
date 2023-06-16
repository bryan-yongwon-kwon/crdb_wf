from storage_workflows.crdb.aws.secret_value import SecretValue
import psycopg2
import os
import stat



class MetadataDBOperations:

    @staticmethod
    def metadata_db_connection():
        dir_path = os.getenv('CRDB_CERTS_DIR_PATH_PREFIX') + "/" + "storage-metadata" + "/"
        ca_cert = os.getenv('ROOT-CA')
        client_cert = os.getenv('CLIENT-CERT')
        client_key = os.getenv('CLIENT-KEY')
        MetadataDBOperations.write_to_file(dir_path, "ca.crt", ca_cert)
        MetadataDBOperations.write_to_file(dir_path, "client.storage_metadata_app_20230509.crt", client_cert)
        MetadataDBOperations.write_to_file(dir_path, "client.storage_metadata_app_20230509.key", client_key)

        connection = psycopg2.connect(
            database="crdb_workflows",
            port="26257",
            user="storage_metadata_app_20230509",
            host="storage-metadata-crdb.us-west-2.aws.ddnw.net",
            sslmode="require",
            sslrootcert=dir_path+"ca.crt",
            sslcert=dir_path+"client.storage_metadata_app_20230509.crt",
            sslkey=dir_path+"client.storage_metadata_app_20230509.key"
        )
        return connection

    @staticmethod
    def close(connection):
        if connection:
            connection.close()

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
    def execute_sql(connection, sql: str, need_commit: bool = False, need_fetchall: bool = False, need_fetchone: bool = False):
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            if need_commit:
                connection.commit()
        except Exception as error:
            print(error)
            raise
        if need_fetchall:
            return cursor.fetchall()
        if need_fetchone:
            return cursor.fetchone()

    @staticmethod
    def persist_asg_old_instance_ids(cluster_name, instances):
        connection = MetadataDBOperations.metadata_db_connection()
        node_list = ','.join(instances)
        node_list = str('{') + node_list + str('}')
        sql_query = "INSERT INTO clusters_info (cluster_name, instance_ids) VALUES ('{}', '{}') ON CONFLICT (cluster_name) " \
                    "DO UPDATE SET (instance_ids) = ('{}');".format(cluster_name, node_list, node_list)
        MetadataDBOperations.execute_sql(connection, sql_query, need_commit=True)
        return

    @staticmethod
    def get_old_nodes(cluster_name):
        connection = MetadataDBOperations.metadata_db_connection()
        sql_query = "SELECT instance_ids FROM clusters_info WHERE cluster_name ='{}';".format(cluster_name)
        instances = MetadataDBOperations.execute_sql(connection, sql_query, need_fetchone=True)
        print("List of old instances: " + str(instances))
        return instances
