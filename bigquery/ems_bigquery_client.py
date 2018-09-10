from collections import Iterable
from datetime import datetime

from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, QueryJob, TableReference, DatasetReference, TimePartitioning

from bigquery.ems_api_error import EmsApiError
from bigquery.ems_query_config import EmsQueryConfig, EmsQueryPriority
import logging
from bigquery.ems_query_job import EmsQueryJob, EmsQueryState

logger = logging.getLogger(__name__)

RETRY = "-retry-"


class EmsBigqueryClient:

    def __init__(self, project_id: str, location: str = "EU"):
        self.__project_id = project_id
        self.__bigquery_client = bigquery.Client(project_id)
        self.__location = location

    def get_job_list(self, min_creation_time: datetime = None) -> Iterable:
        """
        Args:
            min_creation_time (datetime.datetime, optional):
                If set, only jobs created after or at this timestamp are returned.
                If the datetime has no time zone assumes UTC time.
            max_creation_time (datetime.datetime, optional):
                If set, only jobs created before or at this timestamp are returned.
                If the datetime has no time zone assumes UTC time.
        Yields:
            EmsQueryJob: the next job
        """
        for job in self.__bigquery_client.list_jobs(all_users=True,
                                                    max_results=20,
                                                    min_creation_time=min_creation_time):
            if isinstance(job, QueryJob):
                destination = job.destination
                table_id, dataset_id = (destination.table_id, destination.dataset_id) \
                    if destination is not None else (None, None)

                config = EmsQueryConfig(job.priority,
                                        dataset_id,
                                        table_id,
                                        job.create_disposition,
                                        job.write_disposition)
                yield EmsQueryJob(job.job_id, job.query,
                                  config,
                                  EmsQueryState(job.state),
                                  job.error_result)

    #TODO get jobs with prefix
    def get_failed_jobs(self, job_prefix: str, min_creation_time: datetime) -> list:
        jobs = self.get_job_list(min_creation_time)
        matched_jobs = filter(lambda x: job_prefix in x.job_id and x.is_failed, jobs)
        return list(matched_jobs)

    def relaunch_failed_jobs(self, job_prefix: str, min_creation_time: datetime, retry_limit: int = 3):
        failed_jobs = self.get_failed_jobs(job_prefix, min_creation_time)
        #TODO filter jobs for failures
        self.__launch_query_jobs(failed_jobs, job_prefix, retry_limit)


    def run_async_query(self,
                        query: str,
                        job_id_prefix: str = None,
                        ems_query_config: EmsQueryConfig = EmsQueryConfig(
                            priority=EmsQueryPriority.INTERACTIVE)) -> str:
        return self.__execute_query_job(query=query,
                                        ems_query_config=ems_query_config,
                                        job_id_prefix=job_id_prefix).job_id

    def run_sync_query(self,
                       query: str,
                       ems_query_config: EmsQueryConfig = EmsQueryConfig(priority=EmsQueryPriority.INTERACTIVE)
                       ) -> Iterable:

        logger.info("Sync query executed with priority: %s", ems_query_config.priority)
        try:
            return self.__get_mapped_iterator(
                self.__execute_query_job(query=query,
                                         ems_query_config=ems_query_config).result()
            )
        except GoogleAPIError as e:
            raise EmsApiError("Error caused while running query | {} |: {}!".format(query, e.args[0]))

    def __decorate_id_with_retry(self, job_id: str, job_prefix: str, retry_limit: int):
        retry_counter = 0
        if RETRY in job_id:
            retry_counter = self.__get_retry_counter(job_id, job_prefix)
        prefix_with_retry = job_prefix + RETRY + str(retry_counter + 1)

        if retry_counter >= retry_limit:
            raise RetryLimitExceededError()
        return prefix_with_retry

    def __launch_query_jobs(self, jobs: list, job_prefix: str, retry_limit: int = 3):
        for job in jobs:
            prefix_with_retry = self.__decorate_id_with_retry(job.job_id, job_prefix, retry_limit)
            self.run_async_query(job.query, prefix_with_retry, job.query_config)

    def __get_retry_counter(self, job_id, job_id_prefix):
        return int(job_id[len(job_id_prefix) + len(RETRY)])

    def __execute_query_job(self, query: str, ems_query_config: EmsQueryConfig, job_id_prefix=None) -> QueryJob:
        return self.__bigquery_client.query(query=query,
                                            job_config=(self.__create_job_config(ems_query_config)),
                                            job_id_prefix=job_id_prefix,
                                            location=self.__location)

    def __create_job_config(self, ems_query_config: EmsQueryConfig) -> QueryJobConfig:
        job_config = QueryJobConfig()
        job_config.priority = ems_query_config.priority.value
        job_config.use_legacy_sql = False
        if ems_query_config.destination_table is not None:
            job_config.time_partitioning = TimePartitioning("DAY")
            table_reference = TableReference(
                DatasetReference(
                    self.__project_id,
                    ems_query_config.destination_dataset),
                ems_query_config.destination_table)
            job_config.destination = table_reference
            job_config.write_disposition = ems_query_config.write_disposition.value
            job_config.create_disposition = ems_query_config.create_disposition.value
        return job_config

    @staticmethod
    def __get_mapped_iterator(result: Iterable):
        for row in result:
            yield dict(list(row.items()))


class RetryLimitExceededError(Exception):
    pass
