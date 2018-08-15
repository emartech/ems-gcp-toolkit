from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, QueryJob
from google.cloud.bigquery.table import RowIterator

from bigquery.ems_query_priority import EmsQueryPriority


class EmsBigqueryClient:

    def __init__(self, project_id: str, location: str = "EU"):
        self.__bigquery_client = bigquery.Client(project_id)
        self.__location = location

    def run_async_query(self, query: str, job_id_prefix: str = None,
                        priority: EmsQueryPriority = EmsQueryPriority.INTERACTIVE) -> str:
        return self.__execute_query_job(query=query, priority=priority, job_id_prefix=job_id_prefix).job_id

    def run_sync_query(self, query: str) -> RowIterator:
        for row in self.__execute_query_job(query=query, priority=EmsQueryPriority.INTERACTIVE).result():
            yield row
        # return self.__execute_query_job(query=query, priority=EmsQueryPriority.INTERACTIVE).result()

    def __execute_query_job(self, query: str, priority: EmsQueryPriority, job_id_prefix=None) -> QueryJob:
        job_config = QueryJobConfig()
        job_config.priority = priority
        return self.__bigquery_client.query(query=query,
                                            job_config=job_config,
                                            job_id_prefix=job_id_prefix,
                                            location=self.__location)

# TODO
# return Iterator which contains schema and returns row (dict)
# content of a row should be mapped properly, no indirect mapping as in GCP API
