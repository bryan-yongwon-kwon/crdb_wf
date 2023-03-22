from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
from storage_workflows.crdb.api_gateway.aws_client_gateway import AwsClientGateway

class HealthCheck:

    UNAVAILABLE_RANGES_COUNT_INDEX = 0
    UNDER_REPLICATED_RANGES_COUNT_INDEX = 1
    OVER_REPLICATED_RANGES_COUNT_INDEX = 2

    CHECK_RUNNING_JOBS_SQL = "SELECT * FROM [SHOW JOBS] WHERE status = 'running' and (job_type='ROW LEVEL TTL' or job_type='SCHEMA CHANGE' or job_type='BACKUP' or job_type='RESTORE');"
    CHECK_UNHEALTHY_RANGES_SQL = "SELECT sum(unavailable_ranges), sum(under_replicated_ranges), sum(over_replicated_ranges) FROM system.replication_stats;" 

    @staticmethod
    def running_jobs_exist(connection:CrdbConnection) -> bool:
        jobs = connection.execute_sql(HealthCheck.CHECK_RUNNING_JOBS_SQL, False)
        return False if not jobs else True
    
    @staticmethod
    def unhealthy_ranges_exist(connection:CrdbConnection) -> bool:
        unhealthy_ranges = connection.execute_sql(HealthCheck.CHECK_UNHEALTHY_RANGES_SQL, False)[0]
        unhealthy_ranges_sum = (unhealthy_ranges[HealthCheck.UNAVAILABLE_RANGES_COUNT_INDEX] 
                                + unhealthy_ranges[HealthCheck.UNDER_REPLICATED_RANGES_COUNT_INDEX] 
                                + unhealthy_ranges[HealthCheck.OVER_REPLICATED_RANGES_COUNT_INDEX])
        return True if unhealthy_ranges_sum > 0 else False
    
    @staticmethod
    def instances_not_in_service_exist(auto_scaling_group_aws_client, deployment_env, cluster_name) -> bool:
        filter = [
            {
                'Name': 'tag:crdb_cluster_name',
                'Values': [
                    cluster_name + "_" + deployment_env.value,
                ]
            }
        ]
        auto_scaling_group_list = AutoScalingGroup.find_all_auto_scaling_groups(auto_scaling_group_aws_client, filter)
        list_count = len(auto_scaling_group_list)
        assert list_count == 1, "Should get exact 1 AutoScaling Group. Got {} now.".format(list_count)
        auto_scaling_group = auto_scaling_group_list[0]
        return auto_scaling_group.instances_not_in_service_exist()



        