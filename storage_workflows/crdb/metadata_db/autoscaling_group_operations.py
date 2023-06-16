import psycopg2
import os
import stat

class AutoscalingGroupOperations:

    def persist_asg_old_instance_ids(cluster_name, instances):
        connection = MetadataDBOperations.metadata_db_connection()
        node_list = ','.join(instances)
        node_list = str('{') + node_list + str('}')
        sql_query = "INSERT INTO clusters_info (cluster_name, instance_ids) VALUES ('{}', '{}') ON CONFLICT (cluster_name) " \
                    "DO UPDATE SET (instance_ids) = ('{}');".format(cluster_name, node_list, node_list)
        MetadataDBOperations.execute_sql(connection, sql_query, need_commit=True)
        return



    def get_old_nodes(cluster_name):
        connection = MetadataDBOperations.metadata_db_connection()
        sql_query = "SELECT instance_ids FROM clusters_info WHERE cluster_name ='{}';".format(cluster_name)
        instances = MetadataDBOperations.execute_sql(connection, sql_query, need_fetchone=True)
        print("List of old instances: " + str(instances))
        return instances
