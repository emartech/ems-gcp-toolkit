from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, QueryPriority


class EmsBigqueryClient:

    def __init__(self, project_id: str):
        self.__bigquery_client = bigquery.Client(project_id)

    def submit_query_job(self, query: str):
        job_config = QueryJobConfig()
        job_config.priority = QueryPriority.BATCH
        return self.__bigquery_client.query(query, job_config)
