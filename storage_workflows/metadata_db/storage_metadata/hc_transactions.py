import os
from sqlalchemy.orm import Session
from storage_workflows.logging.logger import Logger
from storage_workflows.metadata_db.storage_metadata.tables.cluster_health_check import ClusterHealthCheck, ClusterHealthCheckWorkflowState

logger = Logger()


def add_cluster_health_check_txn(session: Session, **kwargs):
    """
    Inserts a new row into the cluster_health_check table in the database.

    Parameters:
    - cluster_name (str): Name of the cluster.
    - deployment_env (str): Environment of deployment (e.g., "prod", "staging").
    - region (str): AWS region (e.g., "us-west-2").
    - aws_account_name (str): Name of the AWS account.
    - workflow_id (str): Unique identifier for the workflow.
    - check_type (str): Type of the health check.
    - check_result (str, optional): Result of the health check (e.g., "success", "failure"). Defaults to None.
    - check_output (dict, optional): JSON-serializable dictionary containing the output of the health check. Defaults to None.

    Returns:
    True
    """
    entry = ClusterHealthCheck(**kwargs)
    session.add(entry)

    return True


def initiate_hc_workflow_txn(session: Session, **kwargs):
    """
    Inserts a new row into the cluster_health_check_workflow_state table in the database.

    Parameters:
    - cluster_name (str): Name of the cluster.
    - deployment_env (str): Environment of deployment (e.g., "prod", "staging").
    - region (str): AWS region (e.g., "us-west-2").
    - aws_account_name (str): Name of the AWS account.
    - workflow_id (str): Unique identifier for the workflow.
    - check_type (str): Type of the health check.
    - exec_time (ts): Time of execution. Defaults to now().
    - status (str): Workflow step. 'Success', 'Failure', 'Failed', 'InProgress'
    - retry_count (int): number of retries attempted

    Returns:
    True
    """
    entry = ClusterHealthCheckWorkflowState(**kwargs)
    session.add(entry)

    return True


def get_hc_workflow_id_state_txn(session: Session, workflow_id):
    """
    Retrieves healthcheck workflow state

    Parameters:
    - workflow_id (str): Unique identifier for the workflow.

    Returns:
    ClusterHealthCheckWorkflowState
    """
    workflow_state = session.query(ClusterHealthCheckWorkflowState).filter_by(workflow_id=workflow_id).first()

    return workflow_state


def update_workflow_state_with_retry_txn(session: Session, **kwargs):
    """
    Retrieves healthcheck workflow state

    Parameters:
    - cluster_name (str): Name of the cluster.
    - deployment_env (str): Environment of deployment (e.g., "prod", "staging").
    - region (str): AWS region (e.g., "us-west-2").
    - aws_account_name (str): Name of the AWS account.
    - workflow_id (str): Unique identifier for the workflow.
    - check_type (str): Type of the health check.
    - exec_time (ts): Time of execution. Defaults to now().
    - status (str): Workflow step. 'Success', 'Failure', 'Failed', 'InProgress'
    - retry_count (int): number of retries attempted

    Returns:
    ClusterHealthCheckWorkflowState
    """

    MAX_RETRIES = int(os.getenv('MAX-RETRIES'))

    wf_state = ClusterHealthCheckWorkflowState(**kwargs)

    state = session.query(ClusterHealthCheckWorkflowState).filter_by(workflow_id=wf_state.workflow_id).first()

    # Update retry count and status
    if state.status == 'Failure':
        state.retry_count += 1
    else:
        state.retry_count = 0  # Reset on success

    state.check_type = wf_state.check_type
    state.status = wf_state.status

    # If the method fails more than allowed retries, stop retries
    if state.retry_count > MAX_RETRIES:
        state.status = 'Failed'

    session.merge(state)

    return True


def get_hc_results_txn(session: Session, workflow_id, check_result):
    """
    Retrieves healthcheck workflow state

    Parameters:
    - workflow_id (str): Unique identifier for the workflow.
    - status (str): check_type status

    Returns:
    ClusterHealthCheck
    """
    hc_results = session.query(ClusterHealthCheck).filter_by(workflow_id=workflow_id, check_result=check_result).all()

    return hc_results
