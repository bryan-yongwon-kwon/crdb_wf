from __future__ import annotations
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.models.users.base_user import BaseUser

class SqlUser(BaseUser):

    CREATE_SQL_ROLE = "CREATE ROLE IF NOT EXISTS \"{}\" WITH LOGIN SQLLOGIN VIEWACTIVITY VIEWACTIVITYREDACTED NOCANCELQUERY NOCONTROLCHANGEFEED NOCONTROLJOB NOCREATEDB NOCREATELOGIN NOCREATEROLE NOMODIFYCLUSTERSETTING;"
    GRANT_CONNECT = "GRANT CONNECT ON DATABASE \"{}\" TO \"{}\";"
    GRANT_OTHERS = "USE \"{}\"; GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE * TO \"{}\";"
    ALTER_DEFAULT_PRIVS_ON_TABLES = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT CREATE, DROP, SELECT, INSERT, UPDATE, DELETE ON TABLES TO \"{}\";"
    ALTER_DEFAULT_PRIVS_ON_SEQUENCES = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT CREATE, DROP, SELECT, INSERT, UPDATE, DELETE ON SEQUENCES TO \"{}\";"
    ALTER_DEFAULT_PRIVS_ON_SCHEMAS = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT CREATE, USAGE ON SCHEMAS TO \"{}\";"
    ALTER_DEFAULT_PRIVS_ON_TYPES = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT USAGE ON TYPES TO \"{}\";"
    ALTER_DEFAULT_PRIVS_ON_FUNCTIONS = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT EXECUTE ON FUNCTIONS TO \"{}\";"

    def __init__(self, user_name, cluster_name, db_name):
        super().__init__(user_name, "sql", cluster_name, db_name)

    def create_user(self):
        self.connection.execute_sql(self.CREATE_SQL_ROLE.format(self.user_name), need_fetchall=False,
                                    need_connection_close=False)
        self.connection.execute_sql(self.GRANT_CONNECT.format(self.db_name, self.user_name), need_fetchall=False,
                                    need_connection_close=False)
        self.connection.execute_sql(self.GRANT_OTHERS.format(self.db_name, self.user_name), need_fetchall=False,
                                    need_connection_close=False)
        self.connection.execute_sql(self.ALTER_DEFAULT_PRIVS_ON_TABLES.format(self.db_name, self.user_name),
                                    need_fetchall=False, need_connection_close=False)
        self.connection.execute_sql(self.ALTER_DEFAULT_PRIVS_ON_SEQUENCES.format(self.db_name, self.user_name),
                                    need_fetchall=False, need_connection_close=False)
        self.connection.execute_sql(self.ALTER_DEFAULT_PRIVS_ON_TYPES.format(self.db_name, self.user_name),
                                    need_fetchall=False, need_connection_close=False)
        self.connection.execute_sql(self.ALTER_DEFAULT_PRIVS_ON_FUNCTIONS.format(self.db_name, self.user_name),
                                    need_fetchall=False, need_connection_close=False)
        self.connection.close()
