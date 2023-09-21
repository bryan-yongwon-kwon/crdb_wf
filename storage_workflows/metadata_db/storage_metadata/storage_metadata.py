from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from storage_workflows.metadata_db.storage_metadata.hc_transactions import (add_cluster_health_check_txn,
                                                                            initiate_hc_workflow_txn,
                                                                            get_hc_workflow_id_state_txn,
                                                                            update_workflow_state_with_retry_txn)
from storage_workflows.metadata_db.storage_metadata.user_management_transactions import (add_user_txn, get_user_txn)
from storage_workflows.metadata_db.metadata_db_connection import MetadataDBConnection
from sqlalchemy_cockroachdb import run_transaction
from storage_workflows.logging.logger import Logger

logger = Logger()


class StorageMetadata:

    def __init__(self, max_records=20):
        """
        Establish a connection to the database, creating an Engine instance.
        Arguments:
            conn_string {String} -- CRDB connection string.
        """
        self.engine = create_engine("cockroachdb://",
                                    connect_args=MetadataDBConnection.get_connection_args("storage_metadata",
                                                                                          "storage_metadata_app_20230509"))
        self.max_records = max_records
        self.session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)

    def insert_health_check(self, **kwargs):
        return run_transaction(self.session_factory,
                               lambda session: add_cluster_health_check_txn(session, **kwargs))

    def initiate_hc_workflow(self, **kwargs):
        return run_transaction(self.session_factory,
                               lambda session: initiate_hc_workflow_txn(session, **kwargs))

    def get_hc_workflow_id_state(self, workflow_id):
        return run_transaction(self.session_factory,
                               lambda session: get_hc_workflow_id_state_txn(session, workflow_id))

    def update_workflow_state_with_retry(self, **kwargs):
        return run_transaction(self.session_factory,
                               lambda session: update_workflow_state_with_retry_txn(session, **kwargs))

    def insert_user(self, **kwargs):
        return run_transaction(self.session_factory,
                               lambda session: add_user_txn(session, **kwargs))

    def get_user(self, cluster_name, region, aws_account, database_name, role_name, deployment_env):
        return run_transaction(self.session_factory,
                               lambda session: get_user_txn(session, cluster_name, region, aws_account, database_name,
                                                            role_name, deployment_env))
