from sqlalchemy import create_engine, select
from sqlalchemy_cockroachdb import run_transaction
from sqlalchemy.orm import sessionmaker
from storage_workflows.metadata_db.storage_metadata.hc_transactions import insert_health_check_txn
from storage_workflows.metadata_db.metadata_db_connection import MetadataDBConnection


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
        self.session_factory = sessionmaker(bind=self.engine)

    def insert_health_check(self, cluster_name, deployment_env, region, aws_account_name,
                            workflow_id, check_type, check_result, check_output):
        return run_transaction(self.session_factory,
                               lambda session: insert_health_check_txn(session, cluster_name, deployment_env, region,
                                                                       aws_account_name,
                                                                       workflow_id, check_type, check_result,
                                                                       check_output))
