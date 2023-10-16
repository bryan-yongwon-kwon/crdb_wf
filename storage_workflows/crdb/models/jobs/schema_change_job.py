from __future__ import annotations
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.models.jobs.base_job import BaseJob

class SchemaChangelJob(BaseJob):

    @staticmethod
    def find_all_schema_change_jobs(cluster_name) -> list[SchemaChangelJob]:
        connection = CrdbConnection.get_crdb_connection(cluster_name)
        connection.connect()
        response = connection.execute_sql(BaseJob.FIND_ALL_JOBS_BY_TYPE_SQL.format('SCHEMA CHANGE'),
                                          need_connection_close=False, need_commit=False, auto_commit=True)
        connection.close()
        return list(map(lambda job: SchemaChangelJob(job, cluster_name), response))

    def __init__(self, response, cluster_name):
        super().__init__(response[0], response[1], response[2], cluster_name)
        self._response = response
    