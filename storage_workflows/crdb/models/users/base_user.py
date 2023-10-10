from functools import cached_property

class BaseUser:

    def __init__(self, user_name, user_type, cluster_name, db_name, user_creation_sql_statements, password=None):
        self._user_name = user_name
        self._user_type = user_type
        self._cluster_name = cluster_name
        self._db_name = db_name
        self._password = password
        self._user_creation_sql_statements = user_creation_sql_statements

    @property
    def cluster_name(self):
        return self._cluster_name

    @property
    def user_name(self):
        return self._user_name

    @property
    def user_type(self):
        return self._user_type

    @property
    def db_name(self):
        return self._db_name

    @property
    def password(self):
        return self._password

    @property
    def user_creation_sql_statements(self):
        return self._user_creation_sql_statements

    def create_user(self):
        connection = CrdbConnection.get_crdb_connection(self.cluster_name)
        connection.connect()
        for statement in self.user_creation_sql_statements:
            connection.execute_sql(statement, need_fetchall=False, need_connection_close=False)
        connection.close()
