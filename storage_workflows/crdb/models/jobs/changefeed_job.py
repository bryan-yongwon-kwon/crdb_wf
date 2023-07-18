from __future__ import annotations
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.models.jobs.base_job import BaseJob

class ChangefeedJob(BaseJob):

    REMOVE_COORDINATOR_BY_JOB_ID_SQL = "UPDATE system.jobs SET claim_session_id = NULL WHERE id = '{}';"
    GET_COORDINATOR_BY_JOB_ID_SQL = "SELECT coordinator_id from crdb_internal.jobs WHERE id = '{}';"

    @staticmethod
    def find_all_changefeed_jobs(cluster_name) -> list[ChangefeedJob]:
        connection = CrdbConnection.get_crdb_connection(cluster_name)
        connection.connect()
        response = connection.execute_sql(BaseJob.FIND_ALL_JOBS_BY_TYPE_SQL.format('CHANGEFEED'))
        connection.close()
        return list(map(lambda job: ChangefeedJob(job, cluster_name), response))
    
    def __init__(self, response, cluster_name):
        super().__init__(response[0], response[1], response[2], cluster_name)
        self._response = response
        self._cluster_name = cluster_name
    
    def pause(self):
        self.connection.execute_sql(self.PAUSE_JOB_BY_ID_SQL.format(self.id),
                                    need_commit=True)

    def resume(self):
        self.connection.execute_sql(self.RESUME_JOB_BY_ID_SQL.format(self.id),
                                    need_commit=True)

    def remove_coordinator_node(self):
        self.connection.execute_sql(self.REMOVE_COORDINATOR_BY_JOB_ID_SQL(self.id),
                                    need_commit=True)

    def get_coordinator_node(self):
        self.connection.execute_sql(self.GET_COORDINATOR_BY_JOB_ID_SQL(self.id),
                                    need_commit=True)
