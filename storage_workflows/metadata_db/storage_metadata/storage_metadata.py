from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from storage_workflows.metadata_db.storage_metadata.hc_transactions import add_cluster_health_check_txn
from storage_workflows.metadata_db.metadata_db_connection import MetadataDBConnection
from sqlalchemy_cockroachdb import run_transaction
from storage_workflows.metadata_db.storage_metadata.tables.cluster_health_check import ClusterHealthCheck

engine = create_engine("cockroachdb://",
                            connect_args=MetadataDBConnection.get_connection_args("storage_metadata",
                                                                                  "storage_metadata_app_20230509"))
Session = sessionmaker(bind=engine)


def insert_health_check(**kwargs):
        def callback(session, **kwargs):
            add_cluster_health_check_txn(session, **kwargs)
        run_transaction(Session, callback)
