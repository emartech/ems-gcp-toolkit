from unittest import TestCase
from unittest.mock import patch, Mock, ANY

from google.cloud import bigquery
from google.cloud.bigquery import QueryJob, QueryPriority

from bigquery.ems_bigquery_client import EmsBigqueryClient


class TestEmsBigqueryClient(TestCase):

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_init_clientCreatedInsideWithGivenProjectId(self, bigquery_module_patch: bigquery):
        EmsBigqueryClient("some-project-id")

        bigquery_module_patch.Client.assert_called_once_with("some-project-id")

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_submit_query_job_submitsABatchJobAndReturnsWithQueryJob(self, bigquery_module_patch: bigquery):
        test_query = "HELLO * BELLO"
        client_mock = Mock()
        query_job_mock = Mock(QueryJob)

        bigquery_module_patch.Client.return_value = client_mock
        client_mock.query.return_value = query_job_mock

        ems_bigquery_client = EmsBigqueryClient("some-project-id")
        query_job = ems_bigquery_client.submit_query_job(test_query)

        client_mock.query.assert_called_once_with(test_query, ANY)
        query_mock_result = client_mock.query.call_args_list[0][0][1]

        assert isinstance(query_job, QueryJob)
        assert QueryPriority.BATCH == query_mock_result.priority
