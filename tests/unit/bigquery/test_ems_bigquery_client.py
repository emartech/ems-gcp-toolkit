from collections import Iterable
from datetime import datetime
from unittest import TestCase
from unittest.mock import patch, Mock

from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from google.cloud.bigquery import QueryJob, QueryPriority
from google.cloud.bigquery.table import Row

from bigquery.ems_api_error import EmsApiError
from bigquery.ems_bigquery_client import EmsBigqueryClient, RetryLimitExceededError
from bigquery.ems_query_job import EmsQueryJob, EmsQueryState


class TestEmsBigqueryClient(TestCase):
    QUERY = "HELLO * BELLO"
    JOB_ID = "some-job-id"

    def setUp(self):
        self.client_mock = Mock()
        self.query_job_mock = Mock(QueryJob)

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_run_async_query_submitsBatchQueryAndReturnsJobId(self, bigquery_module_patch: bigquery):
        ems_bigquery_client = self.__setup_client(bigquery_module_patch)

        result_job_id = ems_bigquery_client.run_async_query(self.QUERY)

        bigquery_module_patch.Client.assert_called_once_with("some-project-id")
        arguments = self.client_mock.query.call_args_list[0][1]
        assert self.QUERY == arguments["query"]
        assert arguments["location"] == "EU"
        assert QueryPriority.INTERACTIVE == arguments["job_config"].priority
        assert arguments["job_id_prefix"] is None
        assert result_job_id == "some-job-id"

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_run_async_query_usesCustomLocation(self, bigquery_module_patch: bigquery):
        ems_bigquery_client = self.__setup_client(bigquery_module_patch, location="WONDERLAND")

        ems_bigquery_client.run_async_query(self.QUERY)

        arguments = self.client_mock.query.call_args_list[0][1]
        assert arguments["location"] == "WONDERLAND"

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_run_async_query_submitsBatchQueryWithProperJobIdPrefixAndReturnsWithResultIterator(
            self,
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

        result_rows_iterator = ems_bigquery_client.run_sync_query(self.QUERY)
        result_rows = [row for row in result_rows_iterator]

        arguments = self.client_mock.query.call_args_list[0][1]
        assert self.QUERY == arguments["query"]
        assert arguments["location"] == "EU"
        assert QueryPriority.INTERACTIVE == arguments["job_config"].priority
        assert arguments["job_id_prefix"] is None
        assert isinstance(result_rows_iterator, Iterable)
        assert len(result_rows) == 2
        assert result_rows[0] == {"int_column": 42, "str_column": "hello"}
        assert result_rows[1] == {"int_column": 1024, "str_column": "wonderland"}

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_run_sync_query_wrapsGcpErrors(self, bigquery_module_patch: bigquery):
        ems_bigquery_client = self.__setup_client(bigquery_module_patch)
        self.client_mock.query.side_effect = GoogleAPIError("BOOM!")
        query = "SELECT * FROM `non_existing_dataset.whatever`"

        with self.assertRaises(EmsApiError) as context:
            ems_bigquery_client.run_sync_query(query)

        self.assertIn("Error caused while running query", context.exception.args[0])
        self.assertIn("BOOM!", context.exception.args[0])
        self.assertIn(query, context.exception.args[0])

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_run_sync_query_callsGcpEvenIfResultNotUsed(self, bigquery_module_patch: bigquery):
        ems_bigquery_client = self.__setup_client(bigquery_module_patch, [])

        ems_bigquery_client.run_sync_query(self.QUERY)

        self.client_mock.query.assert_called_once()

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_get_job_list_returnWithEmptyIterator(self, bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        self.client_mock.list_jobs.return_value = []

        ems_bigquery_client = EmsBigqueryClient("some-project-id")
        job_list_iterable = ems_bigquery_client.get_job_list()

        result = list(job_list_iterable)
        assert result == []

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_get_job_list_returnWithEmsQueryJobIterator(self, bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        self.query_job_mock.job_id = "123"
        self.query_job_mock.query = "SELECT 1"
        self.query_job_mock.state = "DONE"
        self.query_job_mock.error_result = None
        self.client_mock.list_jobs.return_value = [self.query_job_mock]

        ems_bigquery_client = EmsBigqueryClient("some-project-id")
        job_list_iterable = ems_bigquery_client.get_job_list()

        result = list(job_list_iterable)
        assert len(result) == 1
        assert isinstance(result[0], EmsQueryJob)
        assert result[0].state == EmsQueryState("DONE")
        assert result[0].job_id == "123"
        assert result[0].query == "SELECT 1"
        assert result[0].is_failed is False

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_get_failed_jobs_returnsEmptyIfNoFailedJobFoundWithTheGivenPrefix(self, bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        self.client_mock.list_jobs.return_value = []

        ems_bigquery_client = EmsBigqueryClient("some-project-id")
        min_creation_time = datetime.now()
        query_jobs = ems_bigquery_client.get_failed_jobs("prefixed", min_creation_time)

        self.assertEqual(query_jobs, [])
        self.client_mock.list_jobs.assert_called_with(all_users=True,
                                                      max_results=20,
                                                      min_creation_time=min_creation_time)

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_get_failed_jobs_returnsFilteredJobs_ifFailedJobFoundWithSpecificJobIdPrefix(
            self,
            bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        failed_prefixed_query_job_mock = self.__create_query_job_mock("prefixed-some-job-id", True)
        succeeded_prefixed_query_job_mock = self.__create_query_job_mock("prefixed-some-job-id", False)
        succeeded_non_prefixed_query_job_mock = self.__create_query_job_mock("some-job-id", False)

        self.client_mock.list_jobs.return_value = [failed_prefixed_query_job_mock,
                                                   succeeded_prefixed_query_job_mock,
                                                   succeeded_non_prefixed_query_job_mock]

        ems_bigquery_client = EmsBigqueryClient("some-project-id")
        jobs = ems_bigquery_client.get_failed_jobs("prefixed", datetime.now())

        self.assertTrue(len(jobs) == 1)
        self.assertEqual(jobs[0].job_id, "prefixed-some-job-id")
        self.assertTrue(jobs[0].is_failed)

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_launch_query_job_startsQueryJob(self, bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        job = EmsQueryJob("prefixed-some-job-id", "SIMPLE QUERY", EmsQueryState.DONE, {})
        jobs = [job]

        ems_bigquery_client = EmsBigqueryClient("some-project-id")
        ems_bigquery_client.launch_query_job(jobs, "prefixed")

        arguments = self.client_mock.query.call_args_list[0][1]
        self.assertEqual(arguments["job_id_prefix"], "prefixed-retry-1")
        self.assertEqual(arguments["query"], "SIMPLE QUERY")

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_launch_query_job_startsQueryJobForAllTheJobs(self, bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        first_job = EmsQueryJob("prefixed-some-job-id", "SIMPLE 1 QUERY", EmsQueryState.DONE, {})
        second_job = EmsQueryJob("prefixed-some-job-id", "SIMPLE 2 QUERY", EmsQueryState.DONE, {})
        jobs = [first_job, second_job]

        ems_bigquery_client = EmsBigqueryClient("some-project-id")
        ems_bigquery_client.launch_query_job(jobs, "prefixed")

        first_call_args = self.client_mock.query.call_args_list[0][1]
        self.assertEqual(first_call_args["job_id_prefix"], "prefixed-retry-1")
        second_call_args = self.client_mock.query.call_args_list[1][1]
        self.assertEqual(second_call_args["job_id_prefix"], "prefixed-retry-1")

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_launch_query_job_startsQueryJobWithIncreasedRetryIndex(self, bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        job = EmsQueryJob("prefixed-retry-1-some-job-id", "SIMPLE QUERY", EmsQueryState.DONE, {})
        jobs = [job]

        ems_bigquery_client = EmsBigqueryClient("some-project-id")
        ems_bigquery_client.launch_query_job(jobs, "prefixed")

        arguments = self.client_mock.query.call_args_list[0][1]
        self.assertEqual(arguments["job_id_prefix"], "prefixed-retry-2")

    @patch("bigquery.ems_bigquery_client.bigquery")
    def test_launch_query_job_raisesExceptionIfRetryCountExceedsTheGivenLimit(self, bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        job = EmsQueryJob("prefixed-retry-2-some-job-id", "SIMPLE QUERY", EmsQueryState.DONE, {})
        jobs = [job]

        ems_bigquery_client = EmsBigqueryClient("some-project-id")

        self.assertRaises(RetryLimitExceededError, ems_bigquery_client.launch_query_job(jobs, "prefixed"))

    def __create_query_job_mock(self, job_id: str, has_error: bool):
        error_result = {'reason': 'someReason', 'location': 'query', 'message': 'error occured'}
        query_job_mock = Mock(QueryJob)
        query_job_mock.job_id = job_id
        query_job_mock.query = "SIMPLE QUERY"
        query_job_mock.state = "DONE"
        query_job_mock.error_result = error_result if has_error else None
        return query_job_mock

    def __setup_client(self, bigquery_module_patch, return_value=None, location=None):
        project_id = "some-project-id"
        bigquery_module_patch.Client.return_value = self.client_mock
        self.client_mock.query.return_value = self.query_job_mock
        self.query_job_mock.job_id = self.JOB_ID
        if location is not None:
            ems_bigquery_client = EmsBigqueryClient(project_id, location)
        else:
            ems_bigquery_client = EmsBigqueryClient(project_id)

        if return_value is not None:
            self.query_job_mock.result.return_value = return_value

        return ems_bigquery_client
