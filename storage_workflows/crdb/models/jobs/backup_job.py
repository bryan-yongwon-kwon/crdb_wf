from __future__ import annotations
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection

class BackupJob:


    FIND_ALL_BACKUP_JOBS_SQL = "SELECT job_id,job_type,status  FROM [SHOW JOBS] WHERE job_type='BACKUP';"

    @staticmethod
    def find_all_backup_running_jobs(cluster_name) -> list[BackupJob]:
        connection = CrdbConnection.get_crdb_connection(cluster_name)
        connection.connect()
        response = connection.execute_sql(BackupJob.FIND_ALL_BACKUP_JOBS_SQL)
        connection.close()
        return list(map(lambda job: BackupJob(job), response))

    def __init__(self, response):
        self._response = response

    @property
    def job_id(self):
        return self._response[0]
    
    @property
    def job_type(self):
        return self._response[1]
    
    @property
    def status(self):
        return self._response[2]