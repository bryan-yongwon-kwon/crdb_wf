from __future__ import annotations
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.models.jobs.base_job import BaseJob
from storage_workflows.logging.logger import Logger
import time

logger = Logger()
class ChangefeedJob(BaseJob):

    REMOVE_COORDINATOR_BY_JOB_ID_SQL = "UPDATE system.jobs SET claim_session_id = NULL WHERE id = '{}';"
    GET_COORDINATOR_BY_JOB_ID_SQL = "SELECT coordinator_id from crdb_internal.jobs WHERE job_id = '{}';"
    GET_CHANGEFEED_METADATA = "SELECT running_status,error,(((high_water_timestamp/1e9)::INT)-NOW()::INT) AS latency,CASE WHEN description like '%initial_scan = ''only''%' then TRUE ELSE FALSE END AS is_initial_scan_only,(finished::INT-now()::INT) as finished_ago_seconds FROM crdb_internal.jobs AS OF SYSTEM TIME FOLLOWER_READ_TIMESTAMP() WHERE job_type = 'CHANGEFEED' AND job_id = '{}';"

    @staticmethod
    def find_all_changefeed_jobs(cluster_name) -> list[ChangefeedJob]:
        connection = CrdbConnection.get_crdb_connection(cluster_name)
        connection.connect()
        response = connection.execute_sql(BaseJob.FIND_ALL_JOBS_BY_TYPE_SQL.format('CHANGEFEED'),
                                          need_connection_close=False, need_commit=False, auto_commit=True)
        connection.close()
        return list(map(lambda job: ChangefeedJob(job, cluster_name), response))
    
    def __init__(self, response, cluster_name):
        super().__init__(response[0], response[1], response[2], cluster_name)
        self._response = response
        self._cluster_name = cluster_name
    
    @property
    def changefeed_metadata(self):
        changefeed_metadata_response = self.connection.execute_sql(self.GET_CHANGEFEED_METADATA.format(self.id),
                                    need_commit=False, need_fetchone=True, need_connection_close=False, auto_commit=True)
        return ChangefeedJob.ChangefeedJobInternalStatus(changefeed_metadata_response)
    
    def pause(self):
        self.connection.execute_sql(self.PAUSE_JOB_BY_ID_SQL.format(self.id),
                                    need_commit=True, need_connection_close=False)

    def resume(self):
        self.connection.execute_sql(self.RESUME_JOB_BY_ID_SQL.format(self.id),
                                    need_commit=True, need_connection_close=False)

    def remove_coordinator_node(self):
        self.connection.execute_sql(self.REMOVE_COORDINATOR_BY_JOB_ID_SQL.format(self.id),
                                    need_commit=True, need_connection_close=False)

    def get_coordinator_node(self):
        return self.connection.execute_sql(self.GET_COORDINATOR_BY_JOB_ID_SQL.format(self.id),
                                    need_commit=False, need_fetchone=True, need_connection_close=False)[0]

    def wait_for_job_to_pause(self):
        job_status = ChangefeedJob.get_latest_job_status(self.id, self._cluster_name)
        while job_status == "pause-requested" or job_status == "running":
            logger.info("Waiting for job {} to pause.".format(self.id))
            logger.info("Current job status for job_id {} : {} ".format(self.id, job_status))
            job_status = ChangefeedJob.get_latest_job_status(self.id, self._cluster_name)
            time.sleep(2)
        if job_status == "failed" or job_status == "canceled":
            logger.warning("Job status for job_id {} : {} ".format(self.id, job_status))

    def wait_for_job_to_resume(self):
        job_status = ChangefeedJob.get_latest_job_status(self.id, self._cluster_name)
        while job_status != "running":
            if job_status == "failed" or job_status == "canceled":
                logger.error("Changefeed job with id {} has status {}".format(self.id, job_status))
                raise Exception("Changefeed job failed or canceled.")
            time.sleep(2)
            job_status = ChangefeedJob.get_latest_job_status(self.id, self._cluster_name)

    # todo: https://doordash.atlassian.net/browse/STORAGE-7195
    @staticmethod
    def get_latest_job_status(job_id, cluster_name):
        connection = CrdbConnection.get_crdb_connection(cluster_name)
        connection.connect()
        response = connection.execute_sql(BaseJob.GET_JOB_BY_ID_SQL.format(job_id),
                                          need_connection_close=False, need_commit=False, auto_commit=True)
        connection.close()
        job = ChangefeedJob(response, cluster_name)
        return job.status
    

    class ChangefeedJobInternalStatus:
        def __init__(self, response):
            self._response = response

        @property
        def running_status(self):
            return self._response[0]

        @property
        def error(self):
            return self._response[1]

        @property
        def latency(self):
            return self._response[2]

        @property
        def is_initial_scan_only(self):
            return self._response[3]

        @property
        def finished_ago_seconds(self):
            return self._response[4]
