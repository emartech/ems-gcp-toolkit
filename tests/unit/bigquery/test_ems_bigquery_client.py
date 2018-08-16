from collections import Iterable
from unittest import TestCase
from unittest.mock import patch, Mock

from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from google.cloud.bigquery import QueryJob, QueryPriority
from google.cloud.bigquery.table import Row

from bigquery.ems_api_error import EmsApiError
from bigquery.ems_bigquery_client import EmsBigqueryClient


class TestEmsBigqueryClient(TestCase):
    QUERY = "HELLO * BELLO"
    JOB_ID = "some-job-id"

    def setUp(self):
        self.client_mock = Mock()
        self.query_job_mock = Mock(QueryJob)

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_init_clientCreatedInsideWithGivenProjectId(self, bigquery_module_patch: bigquery):
        EmsBigqueryClient("some-project-id")

        bigquery_module_patch.Client.assert_called_once_with("some-project-id")

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_run_async_query_submitsBatchQueryAndReturnsJobId(self, bigquery_module_patch: bigquery):
        ems_bigquery_client = self.__setup_client(bigquery_module_patch)

        result_job_id = ems_bigquery_client.run_async_query(self.QUERY)

        arguments = self.client_mock.query.call_args_list[0][1]
        assert self.QUERY == arguments["query"]
        assert "EU" == arguments["location"]
        assert QueryPriority.INTERACTIVE == arguments["job_config"].priority
        assert arguments["job_id_prefix"] is None
        assert "some-job-id" == result_job_id

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_run_async_query_usesCustomLocation(self, bigquery_module_patch: bigquery):
        ems_bigquery_client = self.__setup_client(bigquery_module_patch, location="WONDERLAND")

        ems_bigquery_client.run_async_query(self.QUERY)

        arguments = self.client_mock.query.call_args_list[0][1]
        assert "WONDERLAND" == arguments["location"]

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_run_async_query_submitsBatchQueryWithProperJobIdPrefixAndReturnsWithResultIterator(self,
                                                                                                bigquery_module_patch: bigquery):
        ems_bigquery_client = self.__setup_client(bigquery_module_patch)
        test_job_id_prefix = "some-prefix"

        ems_bigquery_client.run_async_query(query=self.QUERY, job_id_prefix=test_job_id_prefix)

        arguments = self.client_mock.query.call_args_list[0][1]
        assert QueryPriority.INTERACTIVE == arguments["job_config"].priority
        assert test_job_id_prefix == arguments["job_id_prefix"]

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_run_sync_query_submitsInteractiveQueryAndReturnsWithResultIterator(self, bigquery_module_patch: bigquery):
        ems_bigquery_client = self.__setup_client(bigquery_module_patch,
                                                  [
                                                      Row((42, "hello"), {"int_column": 0, "str_column": 1}),
                                                      Row((1024, "wonderland"), {"int_column": 0, "str_column": 1})
                                                  ]
                                                  )

        result_rows = ems_bigquery_client.run_sync_query(self.QUERY)

        first_row = next(result_rows)
        second_row = next(result_rows)
        arguments = self.client_mock.query.call_args_list[0][1]
        assert self.QUERY == arguments["query"]
        assert "EU" == arguments["location"]
        assert QueryPriority.INTERACTIVE == arguments["job_config"].priority
        assert arguments["job_id_prefix"] is None
        assert isinstance(result_rows, Iterable)
        assert first_row == {"int_column": 42, "str_column": "hello"}
        assert second_row == {"int_column": 1024, "str_column": "wonderland"}

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_run_sync_wraps_gcp_errors(self, bigquery_module_patch: bigquery):
        ems_bigquery_client = self.__setup_client(bigquery_module_patch)
        self.client_mock.query.side_effect = GoogleAPIError("BOOM!")

        result = ems_bigquery_client.run_sync_query("SELECT * FROM `non_existing_dataset.whatever`")

        with self.assertRaises(EmsApiError) as context:
            list(result)

        assert "Error caused while running query: {}!".format("BOOM!") in context.exception.args[0]

    def __setup_client(self, bigquery_module_patch, return_value=None, location=None):
        bigquery_module_patch.Client.return_value = self.client_mock
        self.client_mock.query.return_value = self.query_job_mock
        self.query_job_mock.job_id = self.JOB_ID
        if location is not None:
            ems_bigquery_client = EmsBigqueryClient("some-project-id", location)
        else:
            ems_bigquery_client = EmsBigqueryClient("some-project-id")

        if return_value is not None:
            self.query_job_mock.result.return_value = return_value

        return ems_bigquery_client