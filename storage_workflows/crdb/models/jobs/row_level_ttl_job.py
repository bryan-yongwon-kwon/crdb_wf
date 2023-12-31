from __future__ import annotations
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.models.jobs.base_job import BaseJob

class RowLevelTtlJob(BaseJob):

    @staticmethod
    def find_all_row_level_ttl_jobs(cluster_name) -> list[RowLevelTtlJob]:
        connection = CrdbConnection.get_crdb_connection(cluster_name)
        connection.connect()
        response = connection.execute_sql(BaseJob.FIND_ALL_JOBS_BY_TYPE_SQL.format('ROW LEVEL TTL'),
                                          need_connection_close=False, need_commit=False, auto_commit=True)
        connection.close()
        return list(map(lambda job: RowLevelTtlJob(job, cluster_name), response))

    def __init__(self, response, cluster_name):
        super().__init__(response[0], response[1], response[2], cluster_name)
        self._response = response
    