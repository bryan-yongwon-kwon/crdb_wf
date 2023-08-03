from __future__ import annotations
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.models.jobs.base_job import BaseJob
from storage_workflows.logging.logger import Logger

logger = Logger()
class ChangefeedJob(BaseJob):

    REMOVE_COORDINATOR_BY_JOB_ID_SQL = "UPDATE system.jobs SET claim_session_id = NULL WHERE id = '{}';"
    GET_COORDINATOR_BY_JOB_ID_SQL = "SELECT coordinator_id from crdb_internal.jobs WHERE job_id = '{}';"

    @staticmethod
    def find_all_changefeed_jobs(cluster_name) -> list[ChangefeedJob]:
        connection = CrdbConnection.get_crdb_connection(cluster_name)
        connection.connect()
        response = connection.execute_sql(BaseJob.FIND_ALL_JOBS_BY_TYPE_SQL.format('CHANGEFEED'))
        connection.close()
        return list(map(lambda job: ChangefeedJob(job, cluster_name), response))
    
    def __init__(self, response, cluster_name):
        super().__init__(response[0], response[1], response[2], cluster_name)
        self._response = response
        self._cluster_name = cluster_name
    
    def pause(self):
        self.connection.execute_sql(self.PAUSE_JOB_BY_ID_SQL.format(self.id),
                                    need_commit=True)

    def resume(self):
        self.connection.execute_sql(self.RESUME_JOB_BY_ID_SQL.format(self.id),
                                    need_commit=True)

    def remove_coordinator_node(self):
        self.connection.execute_sql(self.REMOVE_COORDINATOR_BY_JOB_ID_SQL.format(self.id),
                                    need_commit=True)

    def get_coordinator_node(self):
        return self.connection.execute_sql(self.GET_COORDINATOR_BY_JOB_ID_SQL.format(self.id),
                                    need_commit=True, need_fetchone=True)[0]

    def wait_for_job_to_pause(self):
        job_status = ChangefeedJob.get_latest_job_status(self.id, self._cluster_name)
        while job_status == "paused-requested" or job_status == "running":
            logger.info("Waiting for job {} to pause.".format(self.id))
            logger.info("Current job status for job_id {} : {} ".format(self.id, job_status))
            job_status = ChangefeedJob.get_latest_job_status(self.id, self._cluster_name)
            time.sleep(2)
        if job_status == "failed" or job_status == "cancelled":
            logger.warning("Job status for job_id {} : {} ".format(self.id, job_status))

    def wait_for_job_to_resume(self):
        job_status = ChangefeedJob.get_latest_job_status(self.id, self._cluster_name)
        while job_status != "running":
            if job_status == "failed" or job_status == "cancelled":
                logger.error("Changefeed job with id {} has status {}".format(self.id, job_status))
                raise Exception("Changefeed job failed or cancelled.")
            time.sleep(2)
            job_status = ChangefeedJob.get_latest_job_status(self.id, self._cluster_name)

    # todo: https://doordash.atlassian.net/browse/STORAGE-7195
    @staticmethod
    def get_latest_job_status(job_id, cluster_name):
        connection = CrdbConnection.get_crdb_connection(cluster_name)
        connection.connect()
        response = connection.execute_sql(BaseJob.GET_JOB_BY_ID_SQL.format(job_id), need_fetchone=True)
        connection.close()
        job = ChangefeedJob(response, cluster_name)
        return job.status
