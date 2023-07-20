from storage_workflows.metadata_db.metadata_db_connection import MetadataDBConnection
from storage_workflows.logging.logger import Logger

logger = Logger()

class MetadataDBOperations:

    def __init__(self):
        self._connection = MetadataDBConnection()

    def persist_old_instance_ids(self, cluster_name, deployment_env, instances):
        self._connection.connect()
        node_list = ','.join(instances)
        node_list = str('{') + node_list + str('}')
        sql_query = "UPSERT INTO clusters_info (cluster_name, deployment_env, instance_ids) VALUES ('{}', '{}', '{}');".format(cluster_name, 
                                                                                                                               deployment_env, 
                                                                                                                               node_list)
        self._connection.execute_sql(sql_query, need_commit=True)
        self._connection.close()
        return

    def get_old_instance_ids(self, cluster_name, deployment_env) -> list[str]:
        self._connection.connect()
        sql_query = "SELECT instance_ids FROM clusters_info WHERE cluster_name ='{}' AND deployment_env='{}';".format(cluster_name, deployment_env)
        instances = self._connection.execute_sql(sql_query, need_fetchone=True)[0]
        self._connection.close()
        logger.info("List of old instances: " + str(instances))
        return instances
