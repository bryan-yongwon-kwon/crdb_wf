# from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from abc import ABCMeta, abstractmethod

class CrdbJob(metaclass=ABCMeta):


    @property
    @abstractmethod
    def job_id():
        pass

    @property
    @abstractmethod
    def job_type():
        pass

    @property
    @abstractmethod
    def status():
        pass
