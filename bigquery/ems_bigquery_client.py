from collections import Iterable

from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, QueryPriority, QueryJob


class EmsBigqueryClient:

    def __init__(self, project_id: str):
        self.__bigquery_client = bigquery.Client(project_id)

    def submit_batch_query(self, query: str) -> str:
        return self.__execute_query_job(query, QueryPriority.BATCH).job_id

    def run_sync_query(self, query: str) -> Iterable:
        return self.__execute_query_job(query, QueryPriority.INTERACTIVE).result()

    def __execute_query_job(self, query: str, priority: QueryPriority) -> QueryJob:
        job_config = QueryJobConfig()
        job_config.priority = priority
        return self.__bigquery_client.query(query, job_config)
