from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_cockroachdb import run_transaction
from storage_workflows.metadata_db.metadata_db_connection import MetadataDBConnection
from storage_workflows.metadata_db.crdb_workflows.transactions import upsert_workflow_txn


class CrdbDbOpsWorkflows:

    def __init__(self):
        """
        Establish a connection to the database, creating an Engine instance.
        """
        self.engine = create_engine("cockroachdb://",
                                    connect_args=MetadataDBConnection.get_connection_args("crdb_dbops", "storage_metadata_app_20230509"))
        self.session_factory = sessionmaker(bind=self.engine)

    def upsert_workflow(self, cluster_name, region, deployment_env, operation_type, operator_name):
        return run_transaction(self.session_factory,
                               lambda session: upsert_workflow_txn(session, cluster_name, region, deployment_env, operation_type, operator_name))


