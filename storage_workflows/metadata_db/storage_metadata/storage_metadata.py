from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from storage_workflows.metadata_db.storage_metadata.hc_transactions import add_cluster_health_check_txn
from storage_workflows.metadata_db.metadata_db_connection import MetadataDBConnection
from sqlalchemy_cockroachdb import run_transaction
from storage_workflows.metadata_db.storage_metadata.tables.cluster_health_check import ClusterHealthCheck



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

    def insert_health_check(self, **kwargs):
        cluster_health_check = ClusterHealthCheck(**kwargs)
        return run_transaction(self.session_factory,
                               lambda session: add_cluster_health_check_txn(session, cluster_name=cluster_health_check.cluster_name,
                                                                            deployment_env=cluster_health_check.deployment_env,
                                                                            region=cluster_health_check.region,
                                                                            aws_account_name=cluster_health_check.aws_account_name,
                                                                            check_type=cluster_health_check.check_type,
                                                                            workflow_id=cluster_health_check.workflow_id,
                                                                            check_result=cluster_health_check.check_result,
                                                                            check_output=cluster_health_check.check_output))
