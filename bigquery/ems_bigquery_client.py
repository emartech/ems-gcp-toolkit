import logging
import re
from collections import Iterable
from datetime import datetime
from typing import List

from google.api_core.exceptions import GoogleAPIError, NotFound, Conflict
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, QueryJob, TableReference, DatasetReference, TimePartitioning, \
    LoadJobConfig, LoadJob, ExtractJobConfig, ExtractJob
from google.cloud.bigquery.schema import _parse_schema_resource, _build_schema_resource

from bigquery.ems_api_error import EmsApiError
from bigquery.job.config.ems_job_config import EmsJobPriority
from bigquery.job.config.ems_load_job_config import EmsLoadJobConfig
from bigquery.job.config.ems_query_job_config import EmsQueryJobConfig
from bigquery.job.ems_extract_job import EmsExtractJob
from bigquery.job.ems_job_state import EmsJobState
from bigquery.job.ems_load_job import EmsLoadJob
from bigquery.job.ems_query_job import EmsQueryJob

LOGGER = logging.getLogger(__name__)

RETRY = "-retry-"


class EmsBigqueryClient:
    def __init__(self, project_id: str, location: str = "EU"):
        self.__project_id = project_id
        self.__bigquery_client = bigquery.Client(project_id, location=location)
        self.__location = location

    @property
    def project_id(self) -> str:
        return self.__project_id

    @property
    def location(self) -> str:
        return self.__location

    def dataset_exists(self, dataset_id: str):
        try:
            self.__bigquery_client.get_dataset(dataset_id)
        except NotFound:
            return False
        return True

    def create_dataset_if_not_exists(self, dataset_id: str):
        dataset = self.__bigquery_client.dataset(dataset_id, self.__project_id)
        try:
            self.__bigquery_client.create_dataset(dataset)
            LOGGER.info("Dataset %s created in project %s", dataset_id, self.__project_id)
        except Conflict:
            LOGGER.info("Dataset %s already exists in project %s", dataset_id, self.__project_id)

    def delete_dataset_if_exists(self, dataset_id: str, delete_contents=False):
        try:
            self.__bigquery_client.delete_dataset(dataset_id, delete_contents)
            LOGGER.info("Dataset %s deleted in project %s", dataset_id, self.__project_id)
        except NotFound:
            LOGGER.info("Dataset %s not found in project %s", dataset_id, self.__project_id)

    def table_exists(self, table: str) -> bool:
        try:
            self.__bigquery_client.get_table(table_ref=TableReference.from_string(table))
            return True
        except NotFound:
            return False

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
            elif isinstance(job, LoadJob):
                destination = job.destination
                table_id, dataset_id, project_id = destination.table_id, destination.dataset_id, destination.project
                schema = {"fields": _build_schema_resource(job.schema)}

                config = EmsLoadJobConfig(schema=schema,
                                          source_uri_template=job.source_uris[0],
                                          destination_project_id=project_id,
                                          destination_dataset=dataset_id,
                                          destination_table=table_id,
                                          create_disposition=job.create_disposition,
                                          write_disposition=job.write_disposition)

                yield EmsLoadJob(job_id=job.job_id,
                                 load_config=config,
                                 state=EmsJobState(job.state),
                                 error_result=None)
            elif isinstance(job, ExtractJob):
                table = f'{job.source.project}.{job.source.dataset_id}.{job.source.table_id}'
                destination_uris = job.destination_uris
                yield EmsExtractJob(job_id=job.job_id,
                                    table=table,
                                    destination_uris=destination_uris,
                                    state=EmsJobState(job.state),
                                    error_result=job.error_result)

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

    def run_async_load_job(self, job_id_prefix: str, config: EmsLoadJobConfig) -> str:
        return self.__bigquery_client.load_table_from_uri(source_uris=config.source_uri_template,
                                                          destination=TableReference(
                                                              DatasetReference(config.destination_project_id,
                                                                               config.destination_dataset),
                                                              config.destination_table),
                                                          job_id_prefix=job_id_prefix,
                                                          location=self.__location,
                                                          job_config=self.__create_load_job_config(config)).job_id

    def run_async_extract_job(self, job_id_prefix: str, table: str, destination_uris: List[str],
                              print_header: bool = False) -> str:

        extract_job_config = ExtractJobConfig()
        extract_job_config.print_header = print_header

        return self.__bigquery_client.extract_table(source=TableReference.from_string(table_id=table),
                                                    destination_uris=destination_uris,
                                                    job_id_prefix=job_id_prefix,
                                                    location=self.__location,
                                                    job_config=extract_job_config).job_id

    def run_sync_query(self,
                       query: str,
                       ems_query_job_config: EmsQueryJobConfig = EmsQueryJobConfig(priority=EmsJobPriority.INTERACTIVE),
                       job_id_prefix: str = None
                       ) -> Iterable:
        LOGGER.info("Sync query executed with priority: %s", ems_query_job_config.priority)
        try:
            return self.__get_mapped_iterator(
                self.__execute_query_job(
                    query=query,
                    ems_query_job_config=ems_query_job_config,
                    job_id_prefix=job_id_prefix
                ).result()
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
        regex = job_id_prefix + RETRY + "([0-9]+)-.+"
        return int(re.search(regex, job_id).group(1))

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
        config.skip_leading_rows = ems_load_job_config.skip_leading_rows
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
