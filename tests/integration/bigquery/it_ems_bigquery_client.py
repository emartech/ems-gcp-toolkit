import os
from unittest import TestCase

from google.cloud import bigquery

from bigquery.ems_api_error import EmsApiError
from bigquery.ems_bigquery_client import EmsBigqueryClient


class ItEmsBigqueryClient(TestCase):

    GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
    SIMPLE_QUERY = "SELECT 1 AS data"

    def setUp(self):
        self.client = EmsBigqueryClient(self.GCP_PROJECT_ID)

    def test_run_sync_query_returnsValidValues(self):

        result = self.client.run_sync_query(self.SIMPLE_QUERY)

        rows = list(result)
        assert 1 == len(rows)
        assert {"data": 1} == rows[0]

    def test_run_sync_query_non_existing_dataset(self):
        result = self.client.run_sync_query("SELECT * FROM `non_existing_dataset.whatever`")

        with self.assertRaises(EmsApiError) as context:
            list(result)

        error_message = context.exception.args[0].lower()
        assert "not found" in error_message
        assert self.GCP_PROJECT_ID in error_message
        assert "non_existing_dataset" in error_message

    def test_run_async_query_submitsJob(self):
        job_id = self.client.run_async_query(self.SIMPLE_QUERY)

        job = bigquery.Client(self.GCP_PROJECT_ID).get_job(job_id)

        assert job.state is not None
