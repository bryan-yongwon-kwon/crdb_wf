from functools import cached_property
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection

class ChangefeedJob:

    @staticmethod
    def find_all_paused_crdb_changefeed_jobs(cluster_name):
        FIND_ALL_CHANGEFEED_JOBS_SQL = "SELECT job_id,status FROM [SHOW CHANGEFEED JOBS];"

        connection = CrdbConnection.get_crdb_connection(cluster_name)
        connection.connect()
        response = connection.execute_sql(FIND_ALL_CHANGEFEED_JOBS_SQL)
        connection.close()
        return list(map(lambda job: ChangefeedJob(job, cluster_name), response))
    
    def __init__(self, response, cluster_name):
        self._response = response
        self._cluster_name = cluster_name

    @property
    def cluster_name(self):
        return self._cluster_name

    @property
    def job_id(self):
        return self._response[0]
    
    @property
    def status(self):
        return self._response[1]
    
    @cached_property
    def connection(self):
        connection = CrdbConnection.get_crdb_connection(self.cluster_name)
        connection.connect()
        return connection