from __future__ import annotations
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.models.jobs.base_job import BaseJob

class ChangefeedJob(BaseJob):

    @staticmethod
    def find_all_paused_crdb_changefeed_jobs(cluster_name) -> list[ChangefeedJob]:
        FIND_ALL_CHANGEFEED_JOBS_SQL = "SELECT job_id,status FROM [SHOW CHANGEFEED JOBS];"

        connection = CrdbConnection.get_crdb_connection(cluster_name)
        connection.connect()
        response = connection.execute_sql(FIND_ALL_CHANGEFEED_JOBS_SQL)
        connection.close()
        return list(map(lambda job: ChangefeedJob(job, cluster_name), response))
    
    def __init__(self, response, cluster_name):
        super().__init__(response[0], response[1], cluster_name)
        self._response = response
        self._cluster_name = cluster_name
    
    def pause(self):
        self.connection.execute_sql(self.PAUSE_JOB_BY_ID_SQL.format(self.job_id))

    def resume(self):
        self.connection.execute_sql(self.RESUME_JOB_BY_ID_SQL.format(self.job_id))