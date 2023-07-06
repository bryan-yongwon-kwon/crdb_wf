import psycopg2
import os
import stat
from storage_workflows.metadata_db.metadata_db_connection import MetadataDBConnection
from storage_workflows.logging.logger import Logger

logger = Logger()

class MetadataDBOperations:

    def __init__(self):
        self._connection = MetadataDBConnection()

    def persist_asg_old_instance_ids(self, cluster_name, instances):
        self._connection.connect()
        node_list = ','.join(instances)
        node_list = str('{') + node_list + str('}')
        sql_query = "INSERT INTO clusters_info (cluster_name, instance_ids) VALUES ('{}', '{}') ON CONFLICT (cluster_name) " \
                    "DO UPDATE SET (instance_ids) = ('{}');".format(cluster_name, node_list, node_list)
        self._connection.execute_sql(sql_query, need_commit=True)
        self._connection.close()
        return

    def get_old_nodes(self, cluster_name):
        self._connection.connect()
        sql_query = "SELECT instance_ids FROM clusters_info WHERE cluster_name ='{}';".format(cluster_name)
        instances = self._connection.execute_sql(sql_query, need_fetchone=True)
        self._connection.close()
        logger.info("List of old instances: " + str(instances))
        return instances
