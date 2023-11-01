from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from storage_workflows.metadata_db.crdb_workflows.tables.clusters_info import ClustersInfo
from storage_workflows.logging.logger import Logger


logger = Logger()


def get_instance_ids_txn(session: Session, cluster_name:str, deployment_env:str):
    statement = select(ClustersInfo.instance_ids).where(ClustersInfo.cluster_name == cluster_name,
                                           ClustersInfo.deployment_env == deployment_env)
    crdb_cluster = session.execute(statement).first()
    if not crdb_cluster:
        return None
    
    return crdb_cluster.instance_ids


def upsert_instancer_ids_txn(session: Session, cluster_name:str, deployment_env:str, instance_ids:list[str]):
    insert_statement = insert(ClustersInfo).values(cluster_name=cluster_name,
                                                   deployment_env=deployment_env,
                                                   instance_ids=instance_ids)
    upsert_statement = insert_statement.on_conflict_do_update(constraint='clusters_info_pkey',
                                                              set_=dict(instance_ids=instance_ids))
    session.execute(upsert_statement)

