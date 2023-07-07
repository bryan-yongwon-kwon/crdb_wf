import os
import subprocess
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
from storage_workflows.crdb.models.jobs.backup_job import BackupJob
from storage_workflows.crdb.models.jobs.restore_job import RestorelJob
from storage_workflows.crdb.models.jobs.row_level_ttl_job import RowLevelTtlJob
from storage_workflows.crdb.models.jobs.schema_change_job import SchemaChangelJob
from storage_workflows.crdb.models.node import Node
from storage_workflows.logging.logger import Logger
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection

logger = Logger()
class Cluster:

    def __init__(self):
        self.cluster_name = os.getenv('CLUSTER_NAME')

    @property
    def nodes(self):
        return Node.get_nodes()
    
    def backup_job_is_running(self) -> bool:
        running_backup_jobs = list(filter(lambda job: job.status == 'running',
                                          BackupJob.find_all_backup_running_jobs(self.cluster_name)))
        contains_running_backup_job = any(running_backup_jobs)
        if contains_running_backup_job:
            logger.warning("Running backup job(s) found!")
        return contains_running_backup_job
    
    def restore_job_is_running(self) -> bool:
        running_restore_jobs = list(filter(lambda job: job.status == 'running',
                                           RestorelJob.find_all_restore_running_jobs(self.cluster_name)))
        contains_running_restore_job = any(running_restore_jobs)
        if contains_running_restore_job:
            logger.warning("Running restore job(s) found!")
        return contains_running_restore_job
    
    def schema_change_job_is_running(self) -> bool:
        running_schema_change_jobs = list(filter(lambda job: job.status == 'running',
                                                 SchemaChangelJob.find_all_schema_change_running_jobs(self.cluster_name)))
        contains_schema_change_job = any(running_schema_change_jobs)
        if contains_schema_change_job:
            logger.warning("Running schema change job found!")
        return contains_schema_change_job
    
    def row_level_ttl_job_is_running(self) -> bool:
        running_row_level_ttl_jobs = list(filter(lambda job: job.status == 'running',
                                                 RowLevelTtlJob.find_all_row_level_ttl_running_jobs(self.cluster_name)))
        contains_row_level_ttl_job = any(running_row_level_ttl_jobs)
        if contains_row_level_ttl_job:
            logger.warning("Running row level ttl job(s) found!")
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
            logger.warning("Unhealthy ranges found!")
        return contains_unhealthy_ranges
    
    def instances_not_in_service_exist(self) -> bool:
        return AutoScalingGroup.find_auto_scaling_group_by_cluster_name(self.cluster_name).instances_not_in_service_exist()
    
    def decommission_nodes(self, nodes:list[Node]):
        certs_dir = os.getenv('CRDB_CERTS_DIR_PATH_PREFIX') + "/" + self.cluster_name + "/"
        CrdbConnection.get_crdb_connection(self.cluster_name)
        formatted_cluster_name = "{}-{}".format(self.cluster_name.replace('_', '-'), os.getenv('DEPLOYMENT_ENV'))
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
                                                                                                                              formatted_cluster_name)
            logger.info("Decommissioning nodes with major version {}...".format(major_version))
            result = subprocess.run(node_decommission_command, capture_output=True, shell=True)
            logger.error(result.stderr)
            result.check_returncode()
            logger.info(result.stdout)
            logger.info("Completed decommissioning nodes with major version {}.".format(major_version))