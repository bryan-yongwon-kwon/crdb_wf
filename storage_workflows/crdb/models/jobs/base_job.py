from functools import cached_property
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection


class BaseJob:
    FIND_ALL_JOBS_BY_TYPE_SQL = ("SELECT job_id, job_type, status "
                                 "from crdb_internal.jobs AS OF SYSTEM TIME FOLLOWER_READ_TIMESTAMP() "
                                 "WHERE job_type = '{}' AND ((job_type IS NULL) OR ((job_type NOT IN ('AUTO CREATE STATS', "
                                 "'AUTO SCHEMA TELEMETRY', 'AUTO SPAN CONFIG RECONCILIATION', 'AUTO SQL STATS COMPACTION')) "
                                 "AND ((finished IS NULL) OR (finished > NOW() - INTERVAL '12h' ))));")
    PAUSE_JOB_BY_ID_SQL = "PAUSE JOB {};"
    RESUME_JOB_BY_ID_SQL = "RESUME JOB {};"
    GET_JOB_BY_ID_SQL = "SELECT job_id, job_type, status FROM crdb_internal.jobs AS OF SYSTEM TIME FOLLOWER_READ_TIMESTAMP() WHERE job_id='{}';"
    GET_CHANGEFEED_METADATA = ("SELECT running_status, error, (((high_water_timestamp/1e9)::INT)-NOW()::INT) AS "
                               "latency, CASE WHEN description like '%initial_scan = ''only''%' then TRUE ELSE FALSE "
                               "END AS is_initial_scan_only, (finished::INT-now()::INT) as finished_ago_seconds, "
                               "description, high_water_timestamp FROM crdb_internal.jobs AS OF SYSTEM TIME "
                               "FOLLOWER_READ_TIMESTAMP() WHERE job_type = 'CHANGEFEED' AND job_id = '{}';")

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
    def changefeed_metadata(self):
        result = self.connection.execute_sql(self.GET_CHANGEFEED_METADATA.format(self.id))
        return result[0] if result else None

    @property
    def description(self):
        metadata = self.changefeed_metadata
        return metadata[5] if metadata else None

    @property
    def high_water_timestamp(self):
        metadata = self.changefeed_metadata
        return metadata[6] if metadata else None

    @cached_property
    def connection(self):
        connection = CrdbConnection.get_crdb_connection(self.cluster_name)
        connection.connect()
        return connection
