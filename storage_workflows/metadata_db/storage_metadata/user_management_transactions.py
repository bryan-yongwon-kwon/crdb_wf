import os
from sqlalchemy.orm import Session
from storage_workflows.logging.logger import Logger
from storage_workflows.metadata_db.storage_metadata.tables.user_management import UserManagement

logger = Logger()


def add_user_txn(session: Session, **kwargs):
    """
    Insert a new record into the 'user_management' table.

    Args:
    - cluster_name (str): The name of the cluster.
    - region (str): The AWS region associated with the cluster.
    - aws_account (str): The AWS account ID associated with the cluster.
    - database_name (str): The name of the database. This is to handle shared clusters scenario.
    - role_name (str): The role name.
    - deployment_env (str): The deployment environment (e.g., 'dev', 'prod').
    - certificate_path (str): The path to the certificate file.
    - created_at (str): The timestamp indicating when the record was created (format: 'YYYY-MM-DD HH:MM:SS').
    - updated_at (str): The timestamp indicating when the record was last updated (format: 'YYYY-MM-DD HH:MM:SS').

    Returns:
    True
    """
    entry = UserManagement(**kwargs)
    session.add(entry)

    return True


def get_user_txn(self, cluster_name, region, aws_account, database_name, role_name, deployment_env):
    """
    Retrieve user records from the 'user_management' table based on cluster name and role name.

    Args:
    - cluster_name (str): The name of the cluster.
    - region (str): The AWS region associated with the cluster.
    - aws_account (str): The AWS account ID associated with the cluster.
    - database_name (str): The name of the database. This is to handle shared clusters scenario.
    - role_name (str): The role name.
    - deployment_env (str): The deployment environment (e.g., 'dev', 'prod').

    Returns:
    - List of UserManagement objects matching the criteria.
    """

    try:
        # Use SQLAlchemy query to fetch records based on criteria
        users = session.query(UserManagement).filter_by(cluster_name=cluster_name, role_name=role_name,
                                                        region=region, aws_account=aws_account,
                                                        database_name=database_name, deployment_env=deployment_env).all()

        return users
    except Exception as e:
        logger.info(f"Error: {str(e)}")
        return []
