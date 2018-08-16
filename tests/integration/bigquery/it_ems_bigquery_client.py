import os
from unittest import TestCase

from bigquery.ems_api_error import EmsApiError
from bigquery.ems_bigquery_client import EmsBigqueryClient


class ItEmsBigqueryClient(TestCase):
    GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]

    def test_run_sync_query(self):
        client = EmsBigqueryClient(self.GCP_PROJECT_ID)
        result = client.run_sync_query("SELECT 1 AS data")

        rows = list(result)
        assert 1 == len(rows)
        assert {"data": 1} == rows[0]

    def test_run_sync_query_non_existing_dataset(self):
        client = EmsBigqueryClient(self.GCP_PROJECT_ID)
        result = client.run_sync_query("SELECT * FROM `non_existing_dataset.whatever`")

        with self.assertRaises(EmsApiError) as context:
            list(result)

        error_message = context.exception.args[0].lower()
        assert "not found" in error_message
        assert self.GCP_PROJECT_ID in error_message
        assert "non_existing_dataset" in error_message
