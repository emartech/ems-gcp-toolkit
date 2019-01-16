import logging
from collections import Iterable
from datetime import datetime

from google.cloud.bigquery.schema import _parse_schema_resource

from bigquery.job.config.ems_job_config import EmsJobPriority
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, QueryJob, TableReference, DatasetReference, TimePartitioning, \
    LoadJobConfig

from bigquery.ems_api_error import EmsApiError
from bigquery.job.config.ems_load_job_config import EmsLoadJobConfig
from bigquery.job.config.ems_query_job_config import EmsQueryJobConfig
from bigquery.job.ems_job_state import EmsJobState
from bigquery.job.ems_query_job import EmsQueryJob

logger = logging.getLogger(__name__)

RETRY = "-retry-"


class EmsBigqueryClient:
    def __init__(self, project_id: str, location: str = "EU"):
        self.__project_id = project_id
        self.__bigquery_client = bigquery.Client(project_id)
        self.__location = location

    def get_job_list(self, min_creation_time: datetime = None, max_result: int = 20) -> Iterable:
        """
        Args:
            min_creation_time (datetime.datetime, optional):
                If set, only jobs created after or at this timestamp are returned.
                If the datetime has no time zone assumes UTC time.
            max_result (int, optional):
                Maximum number of jobs to return.
        Yields:
            EmsQueryJob: the next job
        """
        for job in self.__bigquery_client.list_jobs(all_users=True,
                                                    max_results=max_result,
                                                    min_creation_time=min_creation_time):
            if isinstance(job, QueryJob):
                destination = job.destination
                table_id, dataset_id, project_id = \
                    (destination.table_id, destination.dataset_id, destination.project) \
                    if destination is not None else (None, None, None)

                config = EmsQueryJobConfig(priority=EmsJobPriority[job.priority],
                                           destination_project_id=project_id,
                                           destination_dataset=dataset_id,
                                           destination_table=table_id,
                                           create_disposition=job.create_disposition,
                                           write_disposition=job.write_disposition)
                yield EmsQueryJob(job.job_id, job.query,
                                  config,
                                  EmsJobState(job.state),
                                  job.error_result)

    def get_jobs_with_prefix(self, job_prefix: str, min_creation_time: datetime, max_result: int = 20) -> list:
        jobs = self.get_job_list(min_creation_time, max_result)
        matched_jobs = filter(lambda x: job_prefix in x.job_id, jobs)
        return list(matched_jobs)

    def relaunch_failed_jobs(self, job_prefix: str, min_creation_time: datetime, max_attempts: int = 3) -> list:
        def launch(job: EmsQueryJob) -> str:
            prefix_with_retry = self.__decorate_id_with_retry(job.job_id, job_prefix, max_attempts)
            return self.run_async_query(job.query, prefix_with_retry, job.query_config)

        jobs = self.get_jobs_with_prefix(job_prefix, min_creation_time)
        failed_jobs = [x for x in jobs if x.is_failed]
        return [launch(job) for job in failed_jobs]

    def run_async_query(self,
                        query: str,
                        job_id_prefix: str = None,
                        ems_query_job_config: EmsQueryJobConfig = EmsQueryJobConfig(
                            priority=EmsJobPriority.INTERACTIVE)) -> str:
        return self.__execute_query_job(query=query,
                                        ems_query_job_config=ems_query_job_config,
                                        job_id_prefix=job_id_prefix).job_id

    def run_async_load_job(self, source_uri: str, job_id_prefix: str, config: EmsLoadJobConfig) -> str:
        # TODO wrap into own error (do not let google exception to leak)
        return self.__bigquery_client.load_table_from_uri(source_uris=source_uri,
                                                          destination=TableReference(
                                                              DatasetReference(config.destination_project_id,
                                                                               config.destination_dataset),
                                                              config.destination_table),
                                                          job_id_prefix=job_id_prefix,
                                                          location=self.__location,
                                                          job_config=self.__create_load_job_config(config)).job_id

    def run_sync_query(self,
                       query: str,
                       ems_query_job_config: EmsQueryJobConfig = EmsQueryJobConfig(priority=EmsJobPriority.INTERACTIVE)
                       ) -> Iterable:
        logger.info("Sync query executed with priority: %s", ems_query_job_config.priority)
        try:
            return self.__get_mapped_iterator(
                self.__execute_query_job(query=query,
                                         ems_query_job_config=ems_query_job_config).result()
            )
        except GoogleAPIError as e:
            raise EmsApiError("Error caused while running query | {} |: {}!".format(query, e.args[0]))

    def __decorate_id_with_retry(self, job_id: str, job_prefix: str, retry_limit: int):
        retry_counter = 0
        if RETRY in job_id:
            retry_counter = self.__get_retry_counter(job_id, job_prefix)
        prefix_with_retry = job_prefix + RETRY + str(retry_counter + 1) + "-"

        if retry_counter >= retry_limit - 1:
            raise RetryLimitExceededError()
        return prefix_with_retry

    def __get_retry_counter(self, job_id, job_id_prefix):
        return int(job_id[len(job_id_prefix) + len(RETRY)])

    def __execute_query_job(self, query: str, ems_query_job_config: EmsQueryJobConfig, job_id_prefix=None) -> QueryJob:
        return self.__bigquery_client.query(query=query,
                                            job_config=(self.__create_job_config(ems_query_job_config)),
                                            job_id_prefix=job_id_prefix,
                                            location=self.__location)

    def __create_load_job_config(self, ems_load_job_config: EmsLoadJobConfig) -> LoadJobConfig:
        config = LoadJobConfig()
        config.create_disposition = ems_load_job_config.create_disposition.value
        config.write_disposition = ems_load_job_config.write_disposition.value
        config.schema = _parse_schema_resource(ems_load_job_config.schema)
        return config

    def __create_job_config(self, ems_query_job_config: EmsQueryJobConfig) -> QueryJobConfig:
        job_config = QueryJobConfig()
        job_config.priority = ems_query_job_config.priority.value
        job_config.use_legacy_sql = False
        job_config.use_query_cache = ems_query_job_config.use_query_cache
        if ems_query_job_config.destination_table is not None:
            job_config.time_partitioning = TimePartitioning("DAY")
            table_reference = TableReference(
                DatasetReference(ems_query_job_config.destination_project_id or self.__project_id,
                                 ems_query_job_config.destination_dataset),
                ems_query_job_config.destination_table)
            job_config.destination = table_reference
            job_config.write_disposition = ems_query_job_config.write_disposition.value
            job_config.create_disposition = ems_query_job_config.create_disposition.value
        return job_config

    @staticmethod
    def __get_mapped_iterator(result: Iterable):
        for row in result:
            yield dict(list(row.items()))


class RetryLimitExceededError(Exception):
    pass
