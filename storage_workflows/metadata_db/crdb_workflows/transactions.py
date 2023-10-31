from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from storage_workflows.metadata_db.crdb_workflows.tables.clusters_info import ClustersInfo
from storage_workflows.logging.logger import Logger
from storage_workflows.metadata_db.crdb_workflows.tables.changefeed_job_details import ChangefeedJobDetails

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


def _insert_changefeed_job_details_txn(session, job_detail):
    """Insert a new ChangefeedJobDetails record transaction function."""
    session.add(job_detail)


def _get_changefeed_job_details_txn(session, workflow_id):
    """Retrieve ChangefeedJobDetails record by workflow_id transaction function."""
    return session.query(ChangefeedJobDetails).filter(ChangefeedJobDetails.workflow_id == workflow_id).one_or_none()


def _update_changefeed_job_details_txn(session, workflow_id, updated_data):
    """Update ChangefeedJobDetails record by workflow_id transaction function."""
    session.query(ChangefeedJobDetails).filter(ChangefeedJobDetails.workflow_id == workflow_id).update(updated_data)


def _delete_changefeed_job_details_txn(session, workflow_id):
    """Delete ChangefeedJobDetails record by workflow_id transaction function."""
    session.query(ChangefeedJobDetails).filter(ChangefeedJobDetails.workflow_id == workflow_id).delete()
