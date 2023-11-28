from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from storage_workflows.metadata_db.crdb_workflows.tables.clusters_info import ClustersInfo
from storage_workflows.metadata_db.crdb_workflows.tables.changefeed_job_details import ChangefeedJobDetails
from storage_workflows.logging.logger import Logger
from storage_workflows.metadata_db.crdb_workflows.tables.crdb_dbops_wf import CRDBDbOpsWFEntry, WorkflowStatus

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


def get_changefeed_job_details_txn(session: Session, workflow_id:str, job_id:str):
    statement = select(ChangefeedJobDetails.workflow_id,
                       ChangefeedJobDetails.job_id,
                       ChangefeedJobDetails.description,
                       ChangefeedJobDetails.error,
                       ChangefeedJobDetails.high_water_timestamp,
                       ChangefeedJobDetails.is_initial_scan_only,
                       ChangefeedJobDetails.finished_ago_seconds,
                       ChangefeedJobDetails.latency,
                       ChangefeedJobDetails.running_status,
                       ChangefeedJobDetails.status).where(ChangefeedJobDetails.workflow_id == workflow_id,
                                                          ChangefeedJobDetails.job_id == job_id)
    changefeed_job_detail = session.execute(statement).first()
    if not changefeed_job_detail:
        return None
    
    return changefeed_job_detail


def upsert_workflow_txn(self, session, cluster_name, region, deployment_env, operation_type, operator_name):
    new_workflow_entry = CRDBDbOpsWFEntry(
        cluster_name=cluster_name,
        region=region,
        deployment_env=deployment_env,
        operation_type=operation_type,
        operator_name=operator_name,
        status=WorkflowStatus.NEW
    )
    session.add(new_workflow_entry)
    session.commit()
    return new_workflow_entry.id  # Returning the ID of the newly created workflow entry


def upsert_changefeed_job_detail_txn(session: Session, 
                                     workflow_id:str, 
                                     job_id:str, 
                                     description:str, 
                                     error:str, 
                                     high_water_timestamp:str, 
                                     is_initial_scan_only:bool, 
                                     finished_ago_seconds:int, 
                                     latency:int, 
                                     running_status:str, 
                                     status:str):
    insert_statement = insert(ChangefeedJobDetails).values(workflow_id=workflow_id,
                                                           job_id=job_id,
                                                           description=description,
                                                           error=error,
                                                           high_water_timestamp=high_water_timestamp,
                                                           is_initial_scan_only=is_initial_scan_only,
                                                           finished_ago_seconds=finished_ago_seconds,
                                                           latency=latency,
                                                           running_status=running_status,
                                                           status=status)
    upsert_statement = insert_statement.on_conflict_do_update(constraint='changefeed_job_details_pkey',
                                                              set_=dict(description=description,
                                                                        error=error,
                                                                        high_water_timestamp=high_water_timestamp,
                                                                        is_initial_scan_only=is_initial_scan_only,
                                                                        finished_ago_seconds=finished_ago_seconds,
                                                                        latency=latency,
                                                                        running_status=running_status,
                                                                        status=status))
    session.execute(upsert_statement)

