from functools import cached_property

class BaseUser:

    def __init__(self, user_name, user_type, cluster_name, db_name):
        self._user_name = user_name
        self._user_type = user_type
        self._cluster_name = cluster_name
        self._db_name = db_name

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
