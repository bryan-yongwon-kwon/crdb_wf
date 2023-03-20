from storage_workflows.crdb.connect.crdb_connection import CrdbConnection

UNAVAILABLE_RANGES_COUNT_INDEX = 0
UNDER_REPLICATED_RANGES_COUNT_INDEX = 1
OVER_REPLICATED_RANGES_COUNT_INDEX = 2

CHECK_RUNNING_JOBS_SQL = "SELECT * FROM [SHOW JOBS] WHERE status = 'running' and (job_type='ROW LEVEL TTL' or job_type='SCHEMA CHANGE' or job_type='BACKUP' or job_type='RESTORE');"
CHECK_UNHEALTHY_RANGES_SQL = "SELECT sum(unavailable_ranges), sum(under_replicated_ranges), sum(over_replicated_ranges) FROM system.replication_stats;" 


class HealthCheck:

    @staticmethod
    def running_jobs_exist(connection:CrdbConnection) -> bool:
        jobs = connection.execute_sql(CHECK_RUNNING_JOBS_SQL, False)
        return False if not jobs else True
    
    @staticmethod
    def unhealthy_ranges_exist(connection:CrdbConnection) -> bool:
        unhealthy_ranges = connection.execute_sql(CHECK_UNHEALTHY_RANGES_SQL, False)[0]
        unhealthy_ranges_sum = (unhealthy_ranges[UNAVAILABLE_RANGES_COUNT_INDEX] 
                                + unhealthy_ranges[UNDER_REPLICATED_RANGES_COUNT_INDEX] 
                                + unhealthy_ranges[OVER_REPLICATED_RANGES_COUNT_INDEX])
        return True if unhealthy_ranges_sum > 0 else False