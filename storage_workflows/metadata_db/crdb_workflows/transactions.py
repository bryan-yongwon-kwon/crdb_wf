from storage_workflows.metadata_db.crdb_workflows.tables.clusters_info import ClustersInfo

def get_instance_ids_txn(session, cluster_name, deployment_env):
    crdb_cluster = session.query(ClustersInfo).filter(ClustersInfo.cluster_name == cluster_name,
                                                      ClustersInfo.deployment_env == deployment_env).first()
    if not crdb_cluster:
        return None
    
    return crdb_cluster.instance_ids