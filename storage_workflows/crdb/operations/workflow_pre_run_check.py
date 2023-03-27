import os
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
from storage_workflows.crdb.jobs.crdb_backup_job import CrdbBackupJob
from storage_workflows.crdb.jobs.crdb_restore_job import CrdbRestorelJob
from storage_workflows.crdb.jobs.crdb_row_level_ttl_job import CrdbRowLevelTtlJob
from storage_workflows.crdb.jobs.crdb_schema_change_job import CrdbSchemaChangelJob

class WorkflowPreRunCheck:


    UNAVAILABLE_RANGES_COUNT_INDEX = 0
    UNDER_REPLICATED_RANGES_COUNT_INDEX = 1
    OVER_REPLICATED_RANGES_COUNT_INDEX = 2

    CHECK_UNHEALTHY_RANGES_SQL = "SELECT sum(unavailable_ranges), sum(under_replicated_ranges), sum(over_replicated_ranges) FROM system.replication_stats;" 

    @staticmethod
    def backup_job_is_running(cluster_name) -> bool:
        return any(CrdbBackupJob.find_all_crdb_backup_running_jobs(cluster_name))
    
    @staticmethod
    def restore_job_is_running(cluster_name) -> bool:
        return any(CrdbRestorelJob.find_all_crdb_restore_running_jobs(cluster_name))
    
    @staticmethod
    def schema_change_job_is_running(cluster_name) -> bool:
        return any(CrdbSchemaChangelJob.find_crdb_schema_change_running_jobs(cluster_name))
    
    @staticmethod
    def row_level_ttl_job_is_running(cluster_name) -> bool:
        return any(CrdbRowLevelTtlJob.find_all_crdb_row_level_ttl_running_jobs(cluster_name))
    
    @staticmethod
    def unhealthy_ranges_exist(cluster_name) -> bool:
        connection = CrdbConnection.get_crdb_connection(cluster_name)
        connection.connect()
        unhealthy_ranges = connection.execute_sql(WorkflowPreRunCheck.CHECK_UNHEALTHY_RANGES_SQL, False)[0]
        connection.close()
        unhealthy_ranges_sum = (unhealthy_ranges[WorkflowPreRunCheck.UNAVAILABLE_RANGES_COUNT_INDEX] 
                                + unhealthy_ranges[WorkflowPreRunCheck.UNDER_REPLICATED_RANGES_COUNT_INDEX] 
                                + unhealthy_ranges[WorkflowPreRunCheck.OVER_REPLICATED_RANGES_COUNT_INDEX])
        return unhealthy_ranges_sum > 0
    
    @staticmethod
    def instances_not_in_service_exist(cluster_name) -> bool:
        return AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name).instances_not_in_service_exist()
        