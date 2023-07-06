import psycopg2
import os
import stat
from storage_workflows.metadata_db.metadata_db_connection import MetadataDBConnection

logger = Logger()

class AutoscalingGroupOperations:

    def persist_asg_old_instance_ids(cluster_name, instances):
        metadata_db_connection = MetadataDBConnection()
        metadata_db_connection.connect()
        node_list = ','.join(instances)
        node_list = str('{') + node_list + str('}')
        sql_query = "INSERT INTO clusters_info (cluster_name, instance_ids) VALUES ('{}', '{}') ON CONFLICT (cluster_name) " \
                    "DO UPDATE SET (instance_ids) = ('{}');".format(cluster_name, node_list, node_list)
        metadata_db_connection.execute_sql(sql_query, need_commit=True)
        metadata_db_connection.close()
        return

    def get_old_nodes(cluster_name):
        metadata_db_connection = MetadataDBConnection()
        metadata_db_connection.connect()
        sql_query = "SELECT instance_ids FROM clusters_info WHERE cluster_name ='{}';".format(cluster_name)
        instances = connection.execute_sql(sql_query, need_fetchone=True)
        logger.info("List of old instances: " + str(instances))
        return instances
