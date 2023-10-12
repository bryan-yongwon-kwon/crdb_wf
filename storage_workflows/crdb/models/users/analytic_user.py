from __future__ import annotations
from storage_workflows.crdb.models.users.base_user import BaseUser

class AnalyticUser(BaseUser):
    CREATE_ANALYTICS_ROLE = "CREATE ROLE IF NOT EXISTS analytics_exporter WITH LOGIN SQLLOGIN CONTROLCHANGEFEED CONTROLJOB PASSWORD '{}';"
    GRANT_CONNECT_ZONECONFIG = "GRANT CONNECT, ZONECONFIG ON DATABASE \"{}\" TO analytics_exporter;"
    GRANT_SELECT = "USE \"{}\"; GRANT SELECT ON TABLE * TO analytics_exporter;"
    ALTER_DEFAULT_PRIVS_GRANT_SELECT_ON_SEQUENCES = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT SELECT ON SEQUENCES TO analytics_exporter;"
    ALTER_DEFAULT_PRIVS_GRANT_SELECT_ZONECONFIG_ON_TABLES = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT SELECT, ZONECONFIG ON TABLES TO analytics_exporter;"
    ALTER_DEFAULT_PRIVS_GRANT_USAGE_ON_SCHEMAS = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT USAGE ON SCHEMAS TO analytics_exporter;"
    ALTER_DEFAULT_PRIVS_GRANT_USAGE_ON_TYPES = "USE \"{}\"; ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT USAGE ON TYPES TO analytics_exporter;"

    def __init__(self, user_name, cluster_name, db_name, password):
        user_creation_sql_statements = [self.CREATE_ANALYTICS_ROLE.format(password),
                          self.GRANT_CONNECT_ZONECONFIG.format(db_name),
                          self.GRANT_SELECT.format(db_name),
                          self.ALTER_DEFAULT_PRIVS_GRANT_SELECT_ON_SEQUENCES.format(db_name),
                          self.ALTER_DEFAULT_PRIVS_GRANT_SELECT_ZONECONFIG_ON_TABLES.format(db_name),
                          self.ALTER_DEFAULT_PRIVS_GRANT_USAGE_ON_SCHEMAS.format(db_name),
                          self.ALTER_DEFAULT_PRIVS_GRANT_USAGE_ON_TYPES.format(db_name)]
        super().__init__(user_name, "analytics_exporter", cluster_name, db_name, user_creation_sql_statements, password)
