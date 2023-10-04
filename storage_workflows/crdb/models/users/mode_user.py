from __future__ import annotations
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.models.users.base_user import BaseUser


class ModeUser(BaseUser):
    CREATE_MODE_ROLE = "CREATE ROLE IF NOT EXISTS \"{}\" WITH LOGIN VIEWACTIVITYREDACTED PASSWORD '{}';"
    GRANT_CONNECT_ZONECONFIG = "GRANT CONNECT, ZONECONFIG ON DATABASE \"{}\" TO \"{}\";"
    GRANT_SELECT_TABLE = "USE \"{}\"; GRANT SELECT ON TABLE * TO \"{}\";"
    ALTER_DEFAULT_PRIVS_GRANT_SELECT_ON_SEQUENCES = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT SELECT ON SEQUENCES TO \"{}\";"
    ALTER_DEFAULT_PRIVS_ON_TABLES = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT SELECT, ZONECONFIG ON TABLES TO \"{}\";"
    ALTER_DEFAULT_PRIVS_GRANT_USAGE_ON_SCHEMAS = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT USAGE ON SCHEMAS TO \"{}\";"
    ALTER_DEFAULT_PRIVS_GRANT_USAGE_ON_TYPES = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT USAGE ON TYPES TO \"{}\";"

    def __init__(self, user_name, cluster_name, db_name, password):
        super().__init__(user_name, "mode", cluster_name, db_name, password)

    def create_user(self):

        connection = CrdbConnection.get_crdb_connection(self.cluster_name)
        connection.connect()
        connection.execute_sql(self.CREATE_MODE_ROLE.format(self.user_name, self.password), need_fetchall=False,
                                    need_connection_close=False)
        connection.execute_sql(self.GRANT_CONNECT_ZONECONFIG.format(self.db_name, self.user_name), need_fetchall=False,
                                    need_connection_close=False)
        connection.execute_sql(self.GRANT_SELECT_TABLE.format(self.db_name, self.user_name), need_fetchall=False,
                                    need_connection_close=False)
        connection.execute_sql(self.ALTER_DEFAULT_PRIVS_GRANT_SELECT_ON_SEQUENCES.format(self.db_name, self.user_name),
                                    need_fetchall=False, need_connection_close=False)
        connection.execute_sql(self.ALTER_DEFAULT_PRIVS_ON_TABLES.format(self.db_name, self.user_name),
                                    need_fetchall=False, need_connection_close=False)
        connection.execute_sql(self.ALTER_DEFAULT_PRIVS_GRANT_USAGE_ON_SCHEMAS.format(self.db_name, self.user_name),
                                    need_fetchall=False, need_connection_close=False)
        connection.execute_sql(self.ALTER_DEFAULT_PRIVS_GRANT_USAGE_ON_TYPES.format(self.db_name, self.user_name),
                                    need_fetchall=False, need_connection_close=False)
        connection.close()
