from collections import Iterable
from datetime import datetime
from unittest import TestCase
from unittest.mock import patch, Mock

from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from google.cloud.bigquery import QueryJob, QueryPriority, LoadJob, LoadJobConfig, SchemaField, ExtractJob
from google.cloud.bigquery.schema import _parse_schema_resource
from google.cloud.bigquery.table import Row, TableReference

from bigquery.ems_api_error import EmsApiError
from bigquery.ems_bigquery_client import EmsBigqueryClient, RetryLimitExceededError
from bigquery.job.config.ems_job_config import EmsCreateDisposition, EmsWriteDisposition
from bigquery.job.config.ems_load_job_config import EmsLoadJobConfig
from bigquery.job.config.ems_query_job_config import EmsQueryJobConfig
from bigquery.job.ems_job_state import EmsJobState
from bigquery.job.ems_load_job import EmsLoadJob
from bigquery.job.ems_query_job import EmsQueryJob

MIN_CREATION_TIME = datetime(1970, 4, 4)


@patch("bigquery.ems_bigquery_client.bigquery")
class TestEmsBigqueryClient(TestCase):
    QUERY = "HELLO * BELLO"
    JOB_ID = "some-job-id"

    def setUp(self):
        self.client_mock = Mock()
        self.query_job_mock = Mock(QueryJob)
        self.query_job_mock.priority = "INTERACTIVE"
        self.query_config = EmsQueryJobConfig(destination_project_id="some_destination_project_id",
                                              destination_dataset="some_dataset",
                                              destination_table="some_table")

    def test_properties(self, bigquery_module_patch: bigquery):
        ems_bigquery_client = self.__setup_client(bigquery_module_patch, None, "valhalla")

        self.assertEqual(ems_bigquery_client.project_id, "some-project-id")
        self.assertEqual(ems_bigquery_client.location, "valhalla")

    def test_run_async_query_submitsBatchQueryAndReturnsJobId(self, bigquery_module_patch: bigquery):
        ems_bigquery_client = self.__setup_client(bigquery_module_patch)

        result_job_id = ems_bigquery_client.run_async_query(self.QUERY, ems_query_job_config=self.query_config)

        bigquery_module_patch.Client.assert_called_once_with("some-project-id", location="EU")
        arguments = self.client_mock.query.call_args_list[0][1]
        assert arguments["query"] == self.QUERY
        assert arguments["location"] == "EU"
        assert arguments["job_config"].priority == QueryPriority.INTERACTIVE
        self.assertEqual(arguments["job_config"].destination.project, "some_destination_project_id")
        assert arguments["job_id_prefix"] is None
        assert result_job_id == "some-job-id"

    def test_run_async_query_setsDestinationProjectIdToDefaultIfNotGiven(self, bigquery_module_patch: bigquery):
        ems_bigquery_client = self.__setup_client(bigquery_module_patch)
        query_config = EmsQueryJobConfig(destination_project_id=None,
                                         destination_dataset="some_dataset",
                                         destination_table="some_table")

        ems_bigquery_client.run_async_query(self.QUERY, ems_query_job_config=query_config)

        bigquery_module_patch.Client.assert_called_once_with("some-project-id", location="EU")
        arguments = self.client_mock.query.call_args_list[0][1]
        self.assertEqual(arguments["job_config"].destination.project, "some-project-id")

    def test_run_async_query_usesCustomLocation(self, bigquery_module_patch: bigquery):
        ems_bigquery_client = self.__setup_client(bigquery_module_patch, location="WONDERLAND")

        ems_bigquery_client.run_async_query(self.QUERY)

        arguments = self.client_mock.query.call_args_list[0][1]
        assert arguments["location"] == "WONDERLAND"

    def test_run_async_query_submitsBatchQueryWithProperJobIdPrefixAndReturnsWithResultIterator(
            self,
            bigquery_module_patch: bigquery):
        ems_bigquery_client = self.__setup_client(bigquery_module_patch)
        test_job_id_prefix = "some-prefix"

        ems_bigquery_client.run_async_query(query=self.QUERY, job_id_prefix=test_job_id_prefix)

        arguments = self.client_mock.query.call_args_list[0][1]
        assert QueryPriority.INTERACTIVE == arguments["job_config"].priority
        assert test_job_id_prefix == arguments["job_id_prefix"]

    def test_run_async_load_job_submitsLoadJobAndReturnsJobIdWithProperConfig(self, bigquery_module_patch: bigquery):
        project_id = "some-project-id"
        source_uri = "gs://some-source-uri/to_object"
        bigquery_module_patch.Client.return_value = self.client_mock
        input_json_schema = {
            "fields": [{"type": "STRING", "name": "f1"}, {"mode": "REQUIRED", "type": "INTEGER", "name": "f2"}]}
        load_job_config = EmsLoadJobConfig(destination_project_id="some-destination-project-id",
                                           destination_dataset="some-destination-dataset",
                                           destination_table="some-destination-table",
                                           schema=input_json_schema,
                                           source_uri_template=source_uri)
        self.load_job_mock = Mock(LoadJob)
        self.load_job_mock.job_id = self.JOB_ID
        self.client_mock.load_table_from_uri.return_value = self.load_job_mock

        ems_bigquery_client = EmsBigqueryClient(project_id)
        result_job_id = ems_bigquery_client.run_async_load_job("prefix", load_job_config)

        arguments = self.client_mock.load_table_from_uri.call_args_list[0][1]
        self.assertEqual(arguments["source_uris"], source_uri)
        self.assertEqual(arguments["job_id_prefix"], "prefix")
        self.assertEqual(result_job_id, "some-job-id")
        job_config = arguments["job_config"]
        self.assertIsInstance(job_config, LoadJobConfig)
        self.assertEqual(job_config.create_disposition, EmsCreateDisposition.CREATE_IF_NEEDED.value)
        self.assertEqual(job_config.write_disposition, EmsWriteDisposition.WRITE_APPEND.value)

        field1 = SchemaField("f1", "STRING")
        field2 = SchemaField("f2", "INTEGER", "REQUIRED")
        self.assertEqual(job_config.schema, [field1, field2])

    def test_run_async_extract_job_submitsExtractJobAndReturnsJobIdWithProperConfig(self,
                                                                                    bigquery_module_patch: bigquery):
        project_id = "some-project-id"
        table = "some-project.some-dataset.some-table"
        destination_uris = ["gs://some-source-uri/to_object1", "gs://some-source-uri/to_object2"]
        job_prefix = "some_job_prefix"
        bigquery_module_patch.Client.return_value = self.client_mock

        expected_job_id = self.JOB_ID
        self.extract_job_mock = Mock(ExtractJob)
        self.extract_job_mock.job_id = expected_job_id
        self.client_mock.extract_table.return_value = self.extract_job_mock

        ems_bigquery_client = EmsBigqueryClient(project_id)
        result_job_id = ems_bigquery_client.run_async_extract_job(job_id_prefix=job_prefix,
                                                                  table=table,
                                                                  destination_uris=destination_uris)

        self.assertEqual(result_job_id, expected_job_id)


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

    def test_run_sync_query_wrapsGcpErrors(self, bigquery_module_patch: bigquery):
        ems_bigquery_client = self.__setup_client(bigquery_module_patch)
        self.client_mock.query.side_effect = GoogleAPIError("BOOM!")
        query = "SELECT * FROM `non_existing_dataset.whatever`"

        with self.assertRaises(EmsApiError) as context:
            ems_bigquery_client.run_sync_query(query)

        self.assertIn("Error caused while running query", context.exception.args[0])
        self.assertIn("BOOM!", context.exception.args[0])
        self.assertIn(query, context.exception.args[0])

    def test_run_sync_query_callsGcpEvenIfResultNotUsed(self, bigquery_module_patch: bigquery):
        ems_bigquery_client = self.__setup_client(bigquery_module_patch, [])

        ems_bigquery_client.run_sync_query(self.QUERY)

        self.client_mock.query.assert_called_once()

    def test_get_job_list_returnWithEmptyIterator(self, bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        self.client_mock.list_jobs.return_value = []

        ems_bigquery_client = EmsBigqueryClient("some-project-id")
        job_list_iterable = ems_bigquery_client.get_job_list()

        result = list(job_list_iterable)
        assert result == []

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
        assert result[0].state == EmsJobState("DONE")
        assert result[0].job_id == "123"
        assert result[0].query == "SELECT 1"
        assert result[0].is_failed is False
        assert isinstance(result[0].query_config, EmsQueryJobConfig)

    def test_get_job_list_returnWithEmsLoadJobIterator(self, bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        load_job_mock = Mock(LoadJob)
        load_job_mock.job_id = "123"
        load_job_mock.query = "SELECT 1"
        load_job_mock.state = "DONE"
        load_job_mock.error_result = None
        load_job_mock.source_uris = ["gs://some-bucket-id/some-blob-id"]
        destination = Mock(TableReference)
        destination.project = "some-other-project-id"
        destination.dataset_id = "some-destination-dataset"
        destination.table_id = "some-destination-table"
        load_job_mock.destination = destination
        expected_schema = {"fields": [{"description": None, "mode": "NULLABLE", "type": "STRING", "name": "fruit"}]}
        load_job_mock.schema = _parse_schema_resource(expected_schema)

        self.client_mock.list_jobs.return_value = [load_job_mock]

        ems_bigquery_client = EmsBigqueryClient("some-project-id")
        job_list_iterable = ems_bigquery_client.get_job_list()

        result = list(job_list_iterable)
        self.assertEqual(1, len(result))
        result_job = result[0]
        self.assertIsInstance(result_job, EmsLoadJob)
        self.assertEqual(EmsJobState("DONE"), result_job.state)
        self.assertEqual("123", result_job.job_id)
        self.assertFalse(result_job.is_failed)
        self.assertIsInstance(result_job.load_config, EmsLoadJobConfig)
        self.assertEqual("some-other-project-id", result_job.load_config.destination_project_id)
        self.assertEqual("some-destination-dataset", result_job.load_config.destination_dataset)
        self.assertEqual("some-destination-table", result_job.load_config.destination_table)
        self.assertEqual(expected_schema, result_job.load_config.schema)
        self.assertEqual("gs://some-bucket-id/some-blob-id", result_job.load_config.source_uri_template)

    def test_get_job_list_returnsJobWithEmsQueryJobConfigWithoutDestination(self, bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        self.query_job_mock.job_id = "123"
        self.query_job_mock.query = "SELECT 1"
        self.query_job_mock.state = "DONE"
        self.query_job_mock.destination = None
        self.client_mock.list_jobs.return_value = [self.query_job_mock]

        ems_bigquery_client = EmsBigqueryClient("some-project-id")
        job_list_iterable = ems_bigquery_client.get_job_list()

        result = list(job_list_iterable)
        self.assertEqual(result[0].query_config.destination_project_id, None)
        self.assertEqual(result[0].query_config.destination_dataset, None)
        self.assertEqual(result[0].query_config.destination_table, None)

    def test_get_job_list_returnsJobWithEmsQueryJobConfigWithSetDestination(self, bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        self.query_job_mock.job_id = "123"
        self.query_job_mock.query = "SELECT 1"
        self.query_job_mock.state = "DONE"
        destination = Mock(TableReference)
        self.query_job_mock.destination = destination
        destination.project = "some-other-project-id"
        destination.dataset_id = "some-destination-dataset"
        destination.table_id = "some-destination-table"
        self.client_mock.list_jobs.return_value = [self.query_job_mock]

        ems_bigquery_client = EmsBigqueryClient("some-project-id")
        job_list_iterable = ems_bigquery_client.get_job_list()

        result = list(job_list_iterable)
        self.assertEqual(result[0].query_config.destination_project_id, "some-other-project-id")
        self.assertEqual(result[0].query_config.destination_dataset, "some-destination-dataset")
        self.assertEqual(result[0].query_config.destination_table, "some-destination-table")

    def test_get_jobs_with_prefix_returnsEmptyIfNoJobFoundWithTheGivenPrefix(self, bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        self.client_mock.list_jobs.return_value = []

        ems_bigquery_client = EmsBigqueryClient("some-project-id")
        min_creation_time = datetime.now()
        query_jobs = ems_bigquery_client.get_jobs_with_prefix("prefixed", min_creation_time)

        self.assertEqual(query_jobs, [])
        self.client_mock.list_jobs.assert_called_with(all_users=True,
                                                      max_results=20,
                                                      min_creation_time=min_creation_time)

    def test_get_jobs_for_prefix_returnsFilteredJobs_ifJobFoundWithSpecificJobIdPrefix(
            self,
            bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        failed_prefixed_query_job_mock = self.__create_query_job_mock("prefixed-some-job-id1", True)
        succeeded_prefixed_query_job_mock = self.__create_query_job_mock("prefixed-some-job-id2", False)
        succeeded_non_prefixed_query_job_mock = self.__create_query_job_mock("some-job-id", False)

        self.client_mock.list_jobs.return_value = [failed_prefixed_query_job_mock,
                                                   succeeded_prefixed_query_job_mock,
                                                   succeeded_non_prefixed_query_job_mock]

        ems_bigquery_client = EmsBigqueryClient("some-project-id")
        jobs = ems_bigquery_client.get_jobs_with_prefix("prefixed", datetime.now())
        job_ids = [job.job_id for job in jobs]

        self.assertEqual(set(job_ids), {"prefixed-some-job-id1", "prefixed-some-job-id2"})

    def test_relaunch_failed_jobs_startsQueryJob(self, bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        job = self.__create_query_job_mock("prefixed-some-job-id", True)
        self.client_mock.list_jobs.return_value = [job]

        ems_bigquery_client = EmsBigqueryClient("some-project-id")
        ems_bigquery_client.relaunch_failed_jobs("prefixed", MIN_CREATION_TIME)

        arguments = self.client_mock.query.call_args_list[0][1]
        self.assertEqual("prefixed-retry-1-", arguments["job_id_prefix"])
        self.assertEqual(arguments["query"], "SIMPLE QUERY")

    def test_relaunch_failed_jobs_startsQueryJobForAllTheJobs(self, bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        first_job = self.__create_query_job_mock("prefixed-some-job-id", True)
        second_job = self.__create_query_job_mock("prefixed-some-job-id", False)
        self.client_mock.list_jobs.return_value = [first_job, second_job]

        ems_bigquery_client = EmsBigqueryClient("some-project-id")
        ems_bigquery_client.relaunch_failed_jobs("prefixed", MIN_CREATION_TIME)

        self.client_mock.query.assert_called_once()
        first_call_args = self.client_mock.query.call_args_list[0][1]
        self.assertEqual("prefixed-retry-1-", first_call_args["job_id_prefix"])

    def test_relaunch_failed_jobs_startsQueryJobWithIncreasedRetryIndex(self, bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        first_job = self.__create_query_job_mock("prefixed-retry-1-some-job-id", True)
        self.client_mock.list_jobs.return_value = [first_job]

        ems_bigquery_client = EmsBigqueryClient("some-project-id")
        ems_bigquery_client.relaunch_failed_jobs("prefixed", MIN_CREATION_TIME)

        arguments = self.client_mock.query.call_args_list[0][1]
        self.assertEqual("prefixed-retry-2-", arguments["job_id_prefix"])

    def test_relaunch_failed_jobs_canRetryMoreThanNineTimes(self, bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        nineth_job = self.__create_query_job_mock("prefixed-retry-9-some-job-id", True)
        tenth_job = self.__create_query_job_mock("prefixed-retry-10-some-job-id", True)
        self.client_mock.list_jobs.return_value = [nineth_job, tenth_job]

        ems_bigquery_client = EmsBigqueryClient("some-project-id")
        ems_bigquery_client.relaunch_failed_jobs("prefixed", MIN_CREATION_TIME, max_attempts=12)

        arguments = self.client_mock.query.call_args_list[0][1]
        self.assertEqual("prefixed-retry-10-", arguments["job_id_prefix"])

        arguments = self.client_mock.query.call_args_list[1][1]
        self.assertEqual("prefixed-retry-11-", arguments["job_id_prefix"])

    def test_relaunch_failed_jobs_raisesExceptionIfRetryCountExceedsTheGivenLimit(self,
                                                                                  bigquery_module_patch: bigquery):
        bigquery_module_patch.Client.return_value = self.client_mock
        first_job = self.__create_query_job_mock("prefixed-retry-2-some-job-id", True)
        self.client_mock.list_jobs.return_value = [first_job]

        ems_bigquery_client = EmsBigqueryClient("some-project-id")

        self.assertRaises(RetryLimitExceededError,
                          ems_bigquery_client.relaunch_failed_jobs, "prefixed", MIN_CREATION_TIME)

    def __create_query_job_mock(self, job_id: str, has_error: bool):
        error_result = {'reason': 'someReason', 'location': 'query', 'message': 'error occurred'}
        query_job_mock = Mock(QueryJob)
        query_job_mock.job_id = job_id
        query_job_mock.priority = "INTERACTIVE"
        query_job_mock.destination = None
        query_job_mock.query = "SIMPLE QUERY"
        query_job_mock.state = "DONE"
        query_job_mock.error_result = error_result if has_error else None
        return query_job_mock

    def __setup_client(self, bigquery_module_patch, return_value=None, location=None):
        project_id = "some-project-id"
        bigquery_module_patch.Client.return_value = self.client_mock
        self.client_mock.project = "some-project-id"
        self.client_mock.query.return_value = self.query_job_mock
        self.query_job_mock.job_id = self.JOB_ID
        if location is not None:
            ems_bigquery_client = EmsBigqueryClient(project_id, location)
        else:
            ems_bigquery_client = EmsBigqueryClient(project_id)

        if return_value is not None:
            self.query_job_mock.result.return_value = return_value

        return ems_bigquery_client
