from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from storage_workflows.metadata_db.storage_metadata.hc_transactions import add_cluster_health_check_txn
from storage_workflows.metadata_db.metadata_db_connection import MetadataDBConnection
from sqlalchemy_cockroachdb import run_transaction


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
        return run_transaction(self.session_factory,
                               lambda session: add_cluster_health_check_txn(session, **kwargs))
