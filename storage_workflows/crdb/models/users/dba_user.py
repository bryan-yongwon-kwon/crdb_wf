from __future__ import annotations
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.models.users.base_user import BaseUser

class DbaUser(BaseUser):
    CREATE_READ_ONLY_ROLE = "CREATE ROLE IF NOT EXISTS \"{}\" WITH LOGIN SQLLOGIN VIEWACTIVITY VIEWACTIVITYREDACTED NOCANCELQUERY NOCONTROLCHANGEFEED NOCONTROLJOB NOCREATEDB NOCREATELOGIN NOCREATEROLE NOMODIFYCLUSTERSETTING;"
    GRANT_ON_DB = "GRANT CREATE, DROP, CONNECT, ZONECONFIG ON DATABASE \"{}\" TO \"{}\";"
    GRANT_ON_TABLE = "USE \"{}\"; GRANT CREATE, DROP, SELECT, INSERT, UPDATE, DELETE ON TABLE * TO \"{}\";"
    ALTER_DEFAULT_PRIVS_ON_SEQUENCES = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT ALL ON SEQUENCES TO \"{}\";"
    ALTER_DEFAULT_PRIVS_ON_TABLES = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT ALL ON TABLES TO \"{}\";"
    ALTER_DEFAULT_PRIVS_ON_SCHEMAS = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT ALL ON SCHEMAS TO \"{}\";"
    ALTER_DEFAULT_PRIVS_ON_TYPES = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT ALL ON TYPES TO \"{}\";"
    ALTER_DEFAULT_PRIVS_ON_FUNCTIONS = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT EXECUTE ON FUNCTIONS TO \"{}\";"

    def __init__(self, user_name, cluster_name, db_name):
        sql_statements = [self.CREATE_READ_ONLY_ROLE.format(user_name),
                          self.GRANT_ON_DB.format(db_name, user_name),
                          self.GRANT_ON_TABLE.format(db_name, user_name),
                          self.ALTER_DEFAULT_PRIVS_ON_SEQUENCES.format(db_name, user_name),
                          self.ALTER_DEFAULT_PRIVS_ON_TABLES.format(db_name, user_name),
                          self.ALTER_DEFAULT_PRIVS_ON_SCHEMAS.format(db_name, user_name),
                          self.ALTER_DEFAULT_PRIVS_ON_TYPES.format(db_name, user_name),
                          self.ALTER_DEFAULT_PRIVS_ON_FUNCTIONS.format(db_name, user_name),]
        super().__init__(user_name, "dba", cluster_name, db_name, sql_statements)
