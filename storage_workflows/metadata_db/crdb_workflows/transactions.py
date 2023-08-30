from sqlalchemy import select
from sqlalchemy.orm import Session
from storage_workflows.metadata_db.crdb_workflows.tables.clusters_info import ClustersInfo
from storage_workflows.logging.logger import Logger

logger = Logger()

def get_instance_ids_txn(session: Session, cluster_name:str, deployment_env:str):
    statement = select(ClustersInfo.instance_ids).where(ClustersInfo.cluster_name == cluster_name,
                                           ClustersInfo.deployment_env == deployment_env)
    crdb_cluster = session.execute(statement).first()
    logger.info("cluster result: {}".format(crdb_cluster))
    if not crdb_cluster:
        return None
    
    return crdb_cluster.instance_ids