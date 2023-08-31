from storage_workflows.metadata_db.crdb_workflows.crdb_workflows import CrdbWorkflows
from storage_workflows.logging.logger import Logger

logger = Logger()

class MetadataDBOperations:

    def __init__(self):
        self.crdb_workflows_db = CrdbWorkflows()

    def persist_old_instance_ids(self, cluster_name, deployment_env, instances):
        self.crdb_workflows_db.upsert_cluster_instance_ids(cluster_name, deployment_env, instances)

    def get_old_instance_ids(self, cluster_name, deployment_env) -> list[str]:
        return self.crdb_workflows_db.get_cluster_instance_ids(cluster_name, deployment_env)
