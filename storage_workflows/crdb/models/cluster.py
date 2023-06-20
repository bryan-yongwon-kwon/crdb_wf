import os
import subprocess
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
from storage_workflows.crdb.models.jobs.crdb_backup_job import CrdbBackupJob
from storage_workflows.crdb.models.jobs.crdb_restore_job import CrdbRestorelJob
from storage_workflows.crdb.models.jobs.crdb_row_level_ttl_job import CrdbRowLevelTtlJob
from storage_workflows.crdb.models.jobs.crdb_schema_change_job import CrdbSchemaChangelJob
from storage_workflows.crdb.models.node import Node

class Cluster:

    def __init__(self):
        pass

    @property
    def cluster_name(self):
        return os.getenv['CLUSTER_NAME']

    @property
    def nodes(self):
        return Node.get_nodes(self.cluster_name)
    
    def backup_job_is_running(self) -> bool:
        contains_running_backup_job = any(CrdbBackupJob.find_all_crdb_backup_running_jobs(self.cluster_name))
        if contains_running_backup_job:
            print("Running backup job(s) found!")
        return contains_running_backup_job
    
    def restore_job_is_running(self) -> bool:
        contains_running_restore_job = any(CrdbRestorelJob.find_all_crdb_restore_running_jobs(self.cluster_name))
        if contains_running_restore_job:
            print("Running restore job(s) found!")
        return contains_running_restore_job
    
    def schema_change_job_is_running(self) -> bool:
        contains_schema_change_job = any(CrdbSchemaChangelJob.find_crdb_schema_change_running_jobs(self.cluster_name))
        if contains_schema_change_job:
            print("Running schema change job found!")
        return contains_schema_change_job
    
    def row_level_ttl_job_is_running(self) -> bool:
        contains_row_level_ttl_job = any(CrdbRowLevelTtlJob.find_all_crdb_row_level_ttl_running_jobs(self.cluster_name))
        if contains_row_level_ttl_job:
            print("Running row level ttl job(s) found!")
        return contains_row_level_ttl_job
    
    def unhealthy_ranges_exist(self) -> bool:
        UNAVAILABLE_RANGES_COUNT_INDEX = 0
        UNDER_REPLICATED_RANGES_COUNT_INDEX = 1
        OVER_REPLICATED_RANGES_COUNT_INDEX = 2

        CHECK_UNHEALTHY_RANGES_SQL = "SELECT sum(unavailable_ranges), sum(under_replicated_ranges), sum(over_replicated_ranges) FROM system.replication_stats;"

        connection = CrdbConnection.get_crdb_connection(self.cluster_name)
        connection.connect()
        unhealthy_ranges = connection.execute_sql(CHECK_UNHEALTHY_RANGES_SQL, False)[0]
        connection.close()
        unhealthy_ranges_sum = (unhealthy_ranges[UNAVAILABLE_RANGES_COUNT_INDEX] 
                                + unhealthy_ranges[UNDER_REPLICATED_RANGES_COUNT_INDEX] 
                                + unhealthy_ranges[OVER_REPLICATED_RANGES_COUNT_INDEX])
        contains_unhealthy_ranges = unhealthy_ranges_sum > 0
        if contains_unhealthy_ranges:
            print("Unhealthy ranges found!")
        return contains_unhealthy_ranges
    
    def instances_not_in_service_exist(self) -> bool:
        return AutoScalingGroup.find_auto_scaling_group_by_cluster_name(self.cluster_name).instances_not_in_service_exist()
    
    def decommission_nodes(self, nodes:list[Node]):
        certs_dir = os.getenv('CRDB_CERTS_DIR_PATH_PREFIX') + "/" + self.cluster_name + "/"
        cluster_name = "{}-{}".format(self.cluster_name.replace('_', '-'), os.getenv('DEPLOYMENT_ENV'))
        major_version_dict = dict()
        for node in nodes:
            major_version = node.major_version
            node_id = str(node.id)
            if major_version in major_version_dict:
                major_version_dict[major_version].append(node_id)
            else:
                major_version_dict[major_version] = [node_id]
        for major_version in major_version_dict:
            nodes_str = ' '.join(major_version_dict[major_version])
            node_decommission_command = "crdb{} node decommission {} --host={}:26256 --certs-dir={} --cluster-name={}".format(major_version,
                                                                                                                              nodes_str, 
                                                                                                                              nodes[-1].ip_address,
                                                                                                                              certs_dir,
                                                                                                                              cluster_name)
            print("Decommissioning nodes with major version {}...".format(major_version))
            result = subprocess.run(node_decommission_command, capture_output=True, shell=True)
            print(result.stderr)
            result.check_returncode()
            print(result.stdout)
            print("Completed decommissioning nodes with major version {}.".format(major_version))