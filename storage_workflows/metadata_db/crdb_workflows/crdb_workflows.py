from sqlalchemy import create_engine
from sqlalchemy_cockroachdb import run_transaction
from sqlalchemy.orm import sessionmaker
from storage_workflows.metadata_db.crdb_workflows.transactions import get_instance_ids_txn, upsert_instancer_ids_txn, get_changefeed_job_details_txn, upsert_changefeed_job_detail_txn
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
    
    def get_changefeed_job_details(self, workflow_id, job_id):
        return run_transaction(self.session_factory,
                               lambda session: get_changefeed_job_details_txn(session, workflow_id, job_id))
    
    def upsert_changefeed_job_details(self, 
                                      workflow_id, 
                                      job_id, 
                                      description, 
                                      error, 
                                      high_water_timestamp, 
                                      is_initial_scan_only, 
                                      finished_ago_seconds, 
                                      latency, 
                                      running_status, 
                                      status):
        return run_transaction(self.session_factory,
                               lambda session: upsert_changefeed_job_detail_txn(session, 
                                                                                workflow_id, 
                                                                                job_id, 
                                                                                description, 
                                                                                error, 
                                                                                high_water_timestamp, 
                                                                                is_initial_scan_only, 
                                                                                finished_ago_seconds, 
                                                                                latency, 
                                                                                running_status, 
                                                                                status))

