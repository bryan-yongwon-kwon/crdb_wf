import psycopg2
import os


class MetadataDBOperations:

    @staticmethod
    def metadata_db_connection():
        connection = psycopg2.connect(
            database="storage-metadata_prod",
            port="26257",
            user="storage_metadata_app_20230509",
            host="storage-metadata-crdb.us-west-2.aws.ddnw.net",
            sslmode="require",
            sslrootcert=os.getenv('root_ca'),
            sslcert=os.getenv('client_cert'),
            sslkey=os.getenv('client_key')
        )
        return connection

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
    def use_db(connection,database_name):
        sql_query = "use '{}';".format(database_name)
        MetadataDBOperations.execute_sql(connection, sql_query, need_commit=True)
        return

    @staticmethod
    def persist_asg_old_instance_ids(cluster_name, instances):
        connection = MetadataDBOperations.metadata_db_connection(True)
        MetadataDBOperations.use_db(connection, "crdb_workflows")
        node_list = ','.join(instances)
        node_list = str('{') + node_list + str('}')
        sql_query = "INSERT INTO clusters_info (cluster_name, node_list) VALUES ('{}', '{}') ON CONFLICT (cluster_name) " \
                    "DO UPDATE SET (node_list) = ('{}');".format(cluster_name, node_list, node_list)
        MetadataDBOperations.execute_sql(connection, sql_query, need_commit=True)
        return

    @staticmethod
    def get_old_nodes(cluster_name):
        connection = MetadataDBOperations.metadata_db_connection(True)
        MetadataDBOperations.use_db(connection, "crdb_workflows")
        sql_query = "SELECT node_list FROM clusters_info WHERE cluster_name ='{}';".format(cluster_name)
        instances = MetadataDBOperations.execute_sql(connection, sql_query, need_fetchone=True)
        print("List of old instances: " + str(instances))
        return instances
