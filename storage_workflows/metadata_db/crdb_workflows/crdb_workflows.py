from sqlalchemy import create_engine, select
from sqlalchemy_cockroachdb import run_transaction
from sqlalchemy.orm import sessionmaker
from storage_workflows.metadata_db.crdb_workflows.transactions import get_instance_ids_txn
from storage_workflows.metadata_db.metadata_db_connection import MetadataDBConnection

class CrdbWorkflows:

    def __init__(self, max_records=20):
        """
        Establish a connection to the database, creating an Engine instance.
        Arguments:
            conn_string {String} -- CRDB connection string.
        """
        self.engine = create_engine("cockroachdb://", 
                                    connect_args=MetadataDBConnection.get_connection_args("crdb_workflows", "storage_metadata_app_20230509"))
        self.max_records = max_records
        self.sessionfactory = sessionmaker(bind=self.engine)

    def get_cluster_instance_ids(self, cluster_name, deployment_env):
        return run_transaction(
            self.sessionfactory,
            lambda session: get_instance_ids_txn(session, cluster_name, deployment_env))
