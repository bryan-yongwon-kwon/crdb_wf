from __future__ import annotations
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
        sql_statemuser_creation_sql_statementsents = [self.CREATE_MODE_ROLE.format(user_name, self.password),
                          self.GRANT_CONNECT_ZONECONFIG.format(db_name, user_name),
                          self.GRANT_SELECT_TABLE.format(db_name, user_name),
                          self.ALTER_DEFAULT_PRIVS_GRANT_SELECT_ON_SEQUENCES.format(db_name, user_name),
                          self.ALTER_DEFAULT_PRIVS_ON_TABLES.format(db_name, user_name),
                          self.ALTER_DEFAULT_PRIVS_GRANT_USAGE_ON_SCHEMAS.format(db_name, user_name),
                          self.ALTER_DEFAULT_PRIVS_GRANT_USAGE_ON_TYPES.format(db_name, user_name)]
        super().__init__(user_name, "mode", cluster_name, db_name, user_creation_sql_statements, password)
