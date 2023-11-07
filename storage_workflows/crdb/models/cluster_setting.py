from __future__ import annotations
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection

class ClusterSetting:

    @staticmethod
    def find_all_cluster_settings(cluster_name: str) -> list[ClusterSetting]:
        SHOW_ALL_CLUSTER_SETTINGS_SQL = "SHOW CLUSTER SETTINGS"
        connection = CrdbConnection.get_crdb_connection(cluster_name)
        connection.connect()
        response = connection.execute_sql(SHOW_ALL_CLUSTER_SETTINGS_SQL)
        connection.close()
        return list(map(lambda row: ClusterSetting(row, cluster_name), response))
    
    @staticmethod
    def find_cluster_setting(cluster_name: str, setting_name: str) -> ClusterSetting:
        SHOW_CLUSTER_SETTING_SQL = "SELECT * FROM [SHOW CLUSTER SETTINGS] where variable='{}';".format(setting_name)
        connection = CrdbConnection.get_crdb_connection(cluster_name)
        connection.connect()
        response = connection.execute_sql(SHOW_CLUSTER_SETTING_SQL)
        connection.close()
        return ClusterSetting(response[0], cluster_name)
    
    def __init__(self, response, cluster_name: str):
        self._response = response
        self._cluster_name = cluster_name

    @property
    def cluster_name(self):
        return self._cluster_name

    @property
    def variable(self):
        return self._response[0]

    @property
    def value(self):
        return self._response[1]

    @property
    def setting_type(self):
        return self._response[2]

    @property
    def description(self):
        return self._response[3]
    
    def refresh(self):
        SHOW_CLUSTER_SETTING_SQL = "SELECT * FROM [SHOW CLUSTER SETTINGS] where variable='{}';".format(self.variable)
        connection = CrdbConnection.get_crdb_connection(self.cluster_name)
        connection.connect()
        response = connection.execute_sql(SHOW_CLUSTER_SETTING_SQL)
        connection.close()
        self._response = response[0]

    def set_value(self, value):
        SET_CLUSTER_SETTING_SQL = "SET CLUSTER SETTING {} = '{}';".format(self.variable, value)
        connection = CrdbConnection.get_crdb_connection(self.cluster_name)
        connection.connect()
        connection.execute_sql(SET_CLUSTER_SETTING_SQL, auto_commit=True, need_fetchall=False)
        connection.close()
        self.refresh()