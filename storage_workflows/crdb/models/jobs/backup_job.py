from __future__ import annotations
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.models.jobs.base_job import BaseJob

class BackupJob(BaseJob):

    @staticmethod
    def find_all_backup_running_jobs(cluster_name) -> list[BackupJob]:
        connection = CrdbConnection.get_crdb_connection(cluster_name)
        connection.connect()
        response = connection.execute_sql(BaseJob.FIND_ALL_JOBS_BY_TYPE_SQL.format('BACKUP'))
        connection.close()
        return list(map(lambda job: BackupJob(job, cluster_name), response))

    def __init__(self, response, cluster_name):
        super().__init__(response[0], response[2], cluster_name)
        self._response = response
    
    @property
    def job_type(self):
        return self._response[1]