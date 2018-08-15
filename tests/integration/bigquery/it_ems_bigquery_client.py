import os
from unittest import TestCase

from bigquery.ems_bigquery_client import EmsBigqueryClient


class ItEmsBigqueryClient(TestCase):

    def test_run_sync_query(self):
        gcp_project_id = os.environ["GCP_PROJECT_ID"]
        client = EmsBigqueryClient(gcp_project_id)
        result = client.run_sync_query("SELECT 1 AS data")

        rows = list(result)
        assert 1 == len(rows)
        assert {"data": 1} == rows[0]
