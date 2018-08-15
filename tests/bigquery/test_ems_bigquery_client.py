from collections import Iterable
from unittest import TestCase
from unittest.mock import patch, Mock

from google.cloud import bigquery
from google.cloud.bigquery import QueryJob, QueryPriority
from google.cloud.bigquery.table import Row

from bigquery.ems_bigquery_client import EmsBigqueryClient


class TestEmsBigqueryClient(TestCase):

    def setUp(self):
        self.test_query = "HELLO * BELLO"
        self.client_mock = Mock()
        self.query_job_mock = Mock(QueryJob)

        self.client_mock.query.return_value = self.query_job_mock

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_init_clientCreatedInsideWithGivenProjectId(self, bigquery_module_patch: bigquery):
        EmsBigqueryClient("some-project-id")

        bigquery_module_patch.Client.assert_called_once_with("some-project-id")

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_run_async_query_submitsBatchQueryAndReturnsJobId(self, bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        self.query_job_mock.job_id = "some-job-id"
        ems_bigquery_client = EmsBigqueryClient("some-project-id")

        result_job_id = ems_bigquery_client.run_async_query(self.test_query)

        arguments = self.client_mock.query.call_args_list[0][1]

        assert self.test_query == arguments["query"]
        assert "EU" == arguments["location"]
        assert QueryPriority.INTERACTIVE == arguments["job_config"].priority
        assert arguments["job_id_prefix"] is None
        assert "some-job-id" == result_job_id

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_run_async_query_usesCustomLocation(self, bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        self.query_job_mock.job_id = "some-job-id"
        ems_bigquery_client = EmsBigqueryClient("some-project-id", "WONDERLAND")

        ems_bigquery_client.run_async_query(self.test_query)

        arguments = self.client_mock.query.call_args_list[0][1]
        assert "WONDERLAND" == arguments["location"]

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_run_async_query_submitsBatchQueryWithProperJobIdPrefixAndReturnsWithResultIterator(self,
                                                                                                bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        test_job_id_prefix = "some-prefix"
        self.query_job_mock.job_id = test_job_id_prefix + "some-job-id"
        ems_bigquery_client = EmsBigqueryClient("some-project-id")

        result_job_id = ems_bigquery_client.run_async_query(query=self.test_query, job_id_prefix=test_job_id_prefix)

        arguments = self.client_mock.query.call_args_list[0][1]

        assert self.test_query == arguments["query"]
        assert QueryPriority.INTERACTIVE == arguments["job_config"].priority
        assert arguments["job_id_prefix"] is test_job_id_prefix
        assert test_job_id_prefix + "some-job-id" == result_job_id

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_run_sync_query_submitsInteractiveQueryAndReturnsWithResultIterator(self, bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        self.query_job_mock.result.return_value = [Row((1, "hello"), {'x': 0, 'y': 1})]

        ems_bigquery_client = EmsBigqueryClient("some-project-id")
        result_rows = ems_bigquery_client.run_sync_query(self.test_query)

        actual_row = next(result_rows)

        arguments = self.client_mock.query.call_args_list[0][1]

        assert self.test_query == arguments["query"]
        assert "EU" == arguments["location"]
        assert QueryPriority.INTERACTIVE == arguments["job_config"].priority
        assert arguments["job_id_prefix"] is None
        assert isinstance(result_rows, Iterable)
        assert actual_row == Row((1, "hello"), {'x': 0, 'y': 1})
