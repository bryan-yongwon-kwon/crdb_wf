import os
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection

class CrdbChangefeedJob:

    FIND_ALL_PAUSED_CRDB_CHANGEFEED_JOBS_SQL = "SELECT job_id,job_type,status  FROM [SHOW JOBS] WHERE job_type='CHANGEFEED' AND status = 'paused';"

    @staticmethod
    def find_all_paused_crdb_changefeed_jobs(cluster_name):
        connection = CrdbConnection.get_crdb_connection(cluster_name)
        connection.connect()
        response = connection.execute_sql(CrdbChangefeedJob.FIND_ALL_PAUSED_CRDB_CHANGEFEED_JOBS_SQL)
        connection.close()
        return list(map(lambda job: CrdbChangefeedJob(job), response))