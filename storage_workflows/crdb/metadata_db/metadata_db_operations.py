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

    def persist_changefeed_job_details(self, 
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
        self.crdb_workflows_db.upsert_changefeed_job_details(workflow_id, 
                                                             job_id, 
                                                             description, 
                                                             error, 
                                                             high_water_timestamp, 
                                                             is_initial_scan_only, 
                                                             finished_ago_seconds, 
                                                             latency, 
                                                             running_status, 
                                                             status)
        
    def get_changefeed_job_details(self, workflow_id, job_id):
        return self.crdb_workflows_db.get_changefeed_job_details(workflow_id, job_id)