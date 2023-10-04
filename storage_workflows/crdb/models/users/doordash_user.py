from __future__ import annotations
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.models.users.base_user import BaseUser

class DoorDashUser(BaseUser):

    CREATE_DOORDASH_ROLE = "CREATE ROLE IF NOT EXISTS doordash WITH LOGIN NOSQLLOGIN VIEWACTIVITYREDACTED PASSWORD 'doordash';"
    GRANT_CONNECT = "GRANT CONNECT, ZONECONFIG ON DATABASE \"{}\" TO doordash;"
    GRANT_SELECT = "USE \"{}\"; GRANT SELECT ON TABLE * TO doordash;"
    ALTER_DEFAULT_PRIVS_GRANT_SELECT_ON_SEQUENCES = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT SELECT ON SEQUENCES TO doordash;"
    ALTER_DEFAULT_PRIVS_GRANT_SELECT_ZONECONFIG_ON_TABLES = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT SELECT, ZONECONFIG ON TABLES TO doordash;"
    ALTER_DEFAULT_PRIVS_GRANT_USAGE_ON_SCHEMAS = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT USAGE ON SCHEMAS TO doordash;"
    ALTER_DEFAULT_PRIVS_GRANT_USAGE_ON_TYPES = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT USAGE ON TYPES TO doordash;"

    def __init__(self, user_name, cluster_name, db_name):
        super().__init__(user_name, "doordash", cluster_name, db_name)

    def create_user(self):

        connection = CrdbConnection.get_crdb_connection(self.cluster_name)
        connection.connect()
        connection.execute_sql(self.CREATE_DOORDASH_ROLE, need_fetchall=False,
                                    need_connection_close=False)
        connection.execute_sql(self.GRANT_CONNECT.format(self.db_name), need_fetchall=False,
                                    need_connection_close=False)
        connection.execute_sql(self.GRANT_SELECT.format(self.db_name), need_fetchall=False,
                                    need_connection_close=False)
        connection.execute_sql(self.ALTER_DEFAULT_PRIVS_GRANT_SELECT_ON_SEQUENCES.format(self.db_name),
                                    need_fetchall=False, need_connection_close=False)
        connection.execute_sql(self.ALTER_DEFAULT_PRIVS_GRANT_SELECT_ZONECONFIG_ON_TABLES.format(self.db_name),
                                    need_fetchall=False, need_connection_close=False)
        connection.execute_sql(self.ALTER_DEFAULT_PRIVS_GRANT_USAGE_ON_SCHEMAS.format(self.db_name),
                                    need_fetchall=False, need_connection_close=False)
        connection.execute_sql(self.ALTER_DEFAULT_PRIVS_GRANT_USAGE_ON_TYPES.format(self.db_name),
                                    need_fetchall=False, need_connection_close=False)
        connection.close()
