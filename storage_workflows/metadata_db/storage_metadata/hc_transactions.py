from sqlalchemy.orm import Session
from storage_workflows.metadata_db.storage_metadata.tables.cluster_health_check import ClusterHealthCheck
from storage_workflows.logging.logger import Logger

logger = Logger()


def insert_health_check_txn(session: Session, cluster_name: str, deployment_env: str, region: str, aws_account_name: str,
                            workflow_id: str, check_type: str, check_result: str, check_output: str):
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

    session = Session()
    try:
        new_entry = ClusterHealthCheck(
            cluster_name=cluster_name,
            deployment_env=deployment_env,
            region=region,
            aws_account_name=aws_account_name,
            workflow_id=workflow_id,
            check_type=check_type,
            check_result=check_result,
            check_output=check_output
        )
        session.add(new_entry)
        session.commit()
        print("Row inserted successfully!")
    except Exception as e:
        session.rollback()
        print(f"Error inserting row: {e}")
    finally:
        session.close()

    return True
