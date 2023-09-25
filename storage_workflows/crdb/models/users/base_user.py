from functools import cached_property
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection

class BaseUser:

    def __init__(self, user_name, user_type, cluster_name):
        self._user_name = user_name
        self._user_type = user_type
        self._cluster_name = cluster_name

    @property
    def cluster_name(self):
        return self._cluster_name

    @property
    def user_name(self):
        return self._user_name

    @property
    def user_type(self):
        return self._user_type

    @cached_property
    def connection(self):
        connection = CrdbConnection.get_crdb_connection(self.cluster_name)
        connection.connect()
        return connection
