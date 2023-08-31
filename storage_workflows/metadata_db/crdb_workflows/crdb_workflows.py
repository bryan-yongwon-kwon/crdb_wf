from sqlalchemy import create_engine, select
from sqlalchemy_cockroachdb import run_transaction
from sqlalchemy.orm import sessionmaker
from storage_workflows.metadata_db.crdb_workflows.transactions import get_instance_ids_txn, upsert_instancer_ids_txn
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
        self.session_factory = sessionmaker(bind=self.engine)

    def get_cluster_instance_ids(self, cluster_name, deployment_env):
        return run_transaction(self.session_factory,
                               lambda session: get_instance_ids_txn(session, cluster_name, deployment_env))
    
    def upsert_cluster_instance_ids(self, cluster_name, deployment_env, instance_ids):
        return run_transaction(self.session_factory,
                               lambda session: upsert_instancer_ids_txn(session, cluster_name, deployment_env, instance_ids))
