from functools import cached_property
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection

class BaseJob:

    FIND_ALL_JOBS_BY_TYPE_SQL = "SELECT job_id,job_type,status from crdb_internal.jobs AS OF SYSTEM TIME FOLLOWER_READ_TIMESTAMP() WHERE job_type = '{}' AND ((job_type IS NULL) OR ((job_type NOT IN ('AUTO CREATE STATS', 'AUTO SCHEMA TELEMETRY', 'AUTO SPAN CONFIG RECONCILIATION', 'AUTO SQL STATS COMPACTION')) AND ((finished IS NULL) OR (finished > NOW() - INTERVAL '12h' ))));"
    PAUSE_JOB_BY_ID_SQL = "PAUSE JOB {};"
    RESUME_JOB_BY_ID_SQL = "RESUME JOB {};"
    GET_JOB_BY_ID_SQL = "SELECT job_id,job_type,status FROM [SHOW JOBS] WHERE job_id='{}';"

    def __init__(self, job_id, job_type, status, cluster_name):
        self._job_id = job_id
        self._job_type = job_type
        self._status = status
        self._cluster_name = cluster_name
    
    @property
    def cluster_name(self):
        return self._cluster_name

    @property
    def id(self):
        return self._job_id
    
    @property
    def type(self):
        return self._job_type
    
    @property
    def status(self):
        return self._status

    @cached_property
    def connection(self):
        connection = CrdbConnection.get_crdb_connection(self.cluster_name)
        connection.connect()
        return connection
