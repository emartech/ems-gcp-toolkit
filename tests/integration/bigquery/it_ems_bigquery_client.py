from unittest import TestCase

from bigquery.ems_bigquery_client import EmsBigqueryClient


class ItEmsBigqueryClient(TestCase):

    def test_run_sync_query(self):
        client = EmsBigqueryClient("ems-data-platform-dev")
        result = client.run_sync_query("SELECT 1 AS data")

        rows = list(result)
        assert 1 == len(rows)
        assert {"data": 1} == rows[0]

# TODO
# get project id from env
