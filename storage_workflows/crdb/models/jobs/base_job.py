from functools import cached_property
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection

class BaseJob:

    FIND_ALL_JOBS_BY_TYPE_SQL = "SELECT job_id,job_type,status FROM [SHOW JOBS] WHERE job_type='{}';"
    PAUSE_JOB_BY_ID_SQL = "PAUSE JOB {};"
    RESUME_JOB_BY_ID_SQL = "RESUME JOB {};"

    def __init__(self, job_id, status, cluster_name):
        self._job_id = job_id
        self._status = status
        self._cluster_name = cluster_name
    
    @property
    def cluster_name(self):
        return self._cluster_name

    @property
    def job_id(self):
        return self._job_id
    
    @property
    def status(self):
        return self._status

    @cached_property
    def connection(self):
        connection = CrdbConnection.get_crdb_connection(self.cluster_name)
        connection.connect()
        return connection
    