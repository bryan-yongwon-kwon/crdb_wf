from functools import cached_property
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection


class HealthCheck:
    FIND_PTR_SQL = ("select (ts/1000000000)::int::timestamp as \"pts timestamp\", now()-(("
                    "ts/1000000000)::int::timestamp) as \"pts age\", *,crdb_internal.cluster_name() from "
                    "system.protected_ts_records where ((ts/1000000000)::int::timestamp) < now() - interval '2d';")

    def __init__(self, hc_type, status, cluster_name):
        self._hc_type = hc_type
        self._status = status
        self._cluster_name = cluster_name

    @property
    def cluster_name(self):
        return self._cluster_name

    @property
    def type(self):
        return self._hc_type

    @property
    def status(self):
        return self._status

    @cached_property
    def connection(self):
        connection = CrdbConnection.get_crdb_connection(self.cluster_name)
        connection.connect()
        return connection
