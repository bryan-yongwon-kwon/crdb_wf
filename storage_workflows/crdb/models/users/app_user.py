from __future__ import annotations
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.models.users.base_user import BaseUser

class AppUser(BaseUser):
    CREATE_APP_ROLE = "CREATE ROLE IF NOT EXISTS \"{}\" WITH LOGIN SQLLOGIN NOCREATEDB NOCONTROLJOB VIEWACTIVITY CANCELQUERY VIEWACTIVITYREDACTED NOCONTROLCHANGEFEED;"
    GRANT_CONNECT = "GRANT CONNECT ON DATABASE \"{}\" TO \"{}\";"
    GRANT_OTHERS = "USE \"{}\"; GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE * TO \"{}\";"
    ALTER_DEFAULT_PRIVS_ON_TABLES = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT CREATE, DROP, SELECT, INSERT, UPDATE, DELETE ON TABLES TO \"{}\";"
    ALTER_DEFAULT_PRIVS_ON_SEQUENCES = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT CREATE, DROP, SELECT, INSERT, UPDATE, DELETE ON SEQUENCES TO \"{}\";"
    ALTER_DEFAULT_PRIVS_ON_SCHEMAS = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT CREATE, USAGE ON SCHEMAS TO \"{}\";"
    ALTER_DEFAULT_PRIVS_ON_TYPES = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT USAGE ON TYPES TO \"{}\";"
    ALTER_DEFAULT_PRIVS_GRANT_USAGE_ON_SEQUENCES = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT USAGE ON SEQUENCES TO \"{}\";"
    ALTER_DEFAULT_PRIVS_GRANT_EXECUTE_ON_FUNCTIONS = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT EXECUTE ON FUNCTIONS TO \"{}\";"

    def __init__(self, user_name, cluster_name, db_name):
        super().__init__(user_name, "app", cluster_name, db_name)

    def create_user(self):

        connection = CrdbConnection.get_crdb_connection(self.cluster_name)
        connection.connect()
        connection.execute_sql(self.CREATE_APP_ROLE.format(self.user_name), need_fetchall=False,
                                    need_connection_close=False)
        connection.execute_sql(self.GRANT_CONNECT.format(self.db_name, self.user_name), need_fetchall=False,
                                    need_connection_close=False)
        connection.execute_sql(self.GRANT_OTHERS.format(self.db_name, self.user_name), need_fetchall=False,
                                    need_connection_close=False)
        connection.execute_sql(self.ALTER_DEFAULT_PRIVS_ON_TABLES.format(self.db_name, self.user_name),
                                    need_fetchall=False, need_connection_close=False)
        connection.execute_sql(self.ALTER_DEFAULT_PRIVS_ON_SEQUENCES.format(self.db_name, self.user_name),
                                    need_fetchall=False, need_connection_close=False)
        connection.execute_sql(self.ALTER_DEFAULT_PRIVS_ON_SCHEMAS.format(self.db_name, self.user_name),
                                    need_fetchall=False, need_connection_close=False)
        connection.execute_sql(self.ALTER_DEFAULT_PRIVS_ON_TYPES.format(self.db_name, self.user_name),
                                    need_fetchall=False, need_connection_close=False)
        connection.execute_sql(self.ALTER_DEFAULT_PRIVS_GRANT_USAGE_ON_SEQUENCES.format(self.db_name, self.user_name),
                                    need_fetchall=False, need_connection_close=False)
        connection.execute_sql(self.ALTER_DEFAULT_PRIVS_GRANT_EXECUTE_ON_FUNCTIONS.format(self.db_name, self.user_name),
                                    need_fetchall=False, need_connection_close=False)
        connection.close()
