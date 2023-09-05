from storage_workflows.metadata_db.storage_metadata.tables.cluster_health_check import ClusterHealthCheck
from storage_workflows.logging.logger import Logger
from sqlalchemy.orm import Session

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
