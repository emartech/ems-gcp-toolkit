import datetime
import os
import uuid
from unittest import TestCase

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud.bigquery import Dataset, DatasetReference, Table, TableReference, SchemaField, TimePartitioning
from google.cloud.exceptions import Conflict
from tenacity import retry, stop_after_delay

from bigquery.ems_api_error import EmsApiError
from bigquery.ems_bigquery_client import EmsBigqueryClient
from bigquery.job.config.ems_query_job_config import EmsQueryJobConfig


class ItEmsBigqueryClient(TestCase):
    ONE_DAY_IN_MS = 3600000 * 24
    GCP_BIGQUERY_CLIENT = None
    DATASET = None
    GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
    DUMMY_QUERY = "SELECT 1 AS data"
    BAD_QUERY = "VERY BAD QUERY"
    INSERT_TEMPLATE = "INSERT INTO `{}` (int_data, str_data) VALUES (1, 'hello')"
    SELECT_TEMPLATE = "SELECT * FROM `{}`"
    DUMMY_SELECT_TO_TABLE = "SELECT 1 AS int_data, 'hello' AS str_data"

    @classmethod
    def setUpClass(cls):
        cls.GCP_BIGQUERY_CLIENT = bigquery.Client(cls.GCP_PROJECT_ID, location="EU")
        cls.DATASET = cls.__dataset()
        cls.__create_dataset_if_not_exists(cls.DATASET)

    @classmethod
    def __create_dataset_if_not_exists(cls, dataset: Dataset):
        try:
            cls.GCP_BIGQUERY_CLIENT.create_dataset(dataset)
        except Conflict:
            pass

    def setUp(self):
        table_name = "test_table_" + str(int(datetime.datetime.utcnow().timestamp() * 1000))
        table_schema = [SchemaField("int_data", "INT64"), SchemaField("str_data", "STRING")]
        self.table_reference = TableReference(self.DATASET.reference, table_name)
        self.test_table = Table(self.table_reference, table_schema)
        self.test_table.time_partitioning = TimePartitioning("DAY")
        self.__delete_if_exists(self.test_table)
        self.GCP_BIGQUERY_CLIENT.create_table(self.test_table)

        self.client = EmsBigqueryClient(self.GCP_PROJECT_ID)

    def __delete_if_exists(self, table):
        try:
            self.GCP_BIGQUERY_CLIENT.delete_table(table)
        except NotFound:
            pass

    def test_run_sync_query_dummyQuery(self):
        result = self.client.run_sync_query(self.DUMMY_QUERY)

        rows = list(result)
        assert len(rows) == 1
        assert {"data": 1} == rows[0]

    def test_run_sync_query_nonExistingDataset(self):
        with self.assertRaises(EmsApiError) as context:
            self.client.run_sync_query("SELECT * FROM `non_existing_dataset.whatever`")

        error_message = context.exception.args[0].lower()
        assert "not found" in error_message
        assert self.GCP_PROJECT_ID in error_message
        assert "non_existing_dataset" in error_message

    def test_run_sync_query_onExistingData(self):
        query = self.INSERT_TEMPLATE.format(self.__get_table_path())
        self.client.run_sync_query(query)

        query_result = self.client.run_sync_query(self.SELECT_TEMPLATE.format(self.__get_table_path()))

        assert [{"int_data": 1, "str_data": "hello"}] == list(query_result)

    def test_run_sync_query_withDestinationSet(self):
        ems_query_job_config = EmsQueryJobConfig(
            destination_dataset=ItEmsBigqueryClient.DATASET.dataset_id,
            destination_table=self.test_table.table_id
        )
        query_with_destination_result = list(self.client.run_sync_query(self.DUMMY_SELECT_TO_TABLE,
                                                                        ems_query_job_config=ems_query_job_config))
        query_result = list(self.client.run_sync_query(self.SELECT_TEMPLATE.format(self.__get_table_path())))

        assert [{"int_data": 1, "str_data": "hello"}] == query_result
        assert query_with_destination_result == query_result

    def test_run_async_query_submitsJob(self):
        job_id = self.client.run_async_query(self.DUMMY_QUERY)

        job = self.GCP_BIGQUERY_CLIENT.get_job(job_id)

        assert job.state is not None

    def test_run_get_job_list(self):
        unique_id = self.client.run_async_query(self.DUMMY_QUERY)
        jobs_iterator = self.client.get_job_list()
        found = unique_id in [job.job_id for job in jobs_iterator]
        assert found

    def test_run_get_job_list_returns2JobsIfMaxResultSetTo2(self):
        for i in range(1, 3):
            self.client.run_async_query(self.DUMMY_QUERY)
        jobs_iterator = self.client.get_job_list(max_result=2)
        assert 2 == len(list(jobs_iterator))

    def test_get_jobs_with_prefix(self):
        job_prefix = "testprefix" + uuid.uuid4().hex
        id1 = self.client.run_async_query(self.DUMMY_QUERY, job_id_prefix=job_prefix)
        id2 = self.client.run_async_query(self.BAD_QUERY, job_id_prefix=job_prefix)
        id3 = self.client.run_async_query(self.DUMMY_QUERY, job_id_prefix="unique_prefix")

        self.__wait_for_job_submitted(id1)
        self.__wait_for_job_submitted(id2)
        self.__wait_for_job_submitted(id3)

        min_creation_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=1)
        jobs = self.client.get_jobs_with_prefix(job_prefix, min_creation_time)
        job_ids = [job.job_id for job in jobs]

        expected_ids = [id1, id2]
        self.assertSetEqual(set(expected_ids), set(job_ids))

    def test_relaunch_failed_jobs(self):
        job_prefix = "testprefix" + uuid.uuid4().hex
        id1 = self.client.run_async_query(self.DUMMY_QUERY, job_id_prefix=job_prefix)
        id2 = self.client.run_async_query(self.BAD_QUERY, job_id_prefix=job_prefix)
        id3 = self.client.run_async_query(self.BAD_QUERY, job_id_prefix="unique_prefix")

        self.__wait_for_job_submitted(id1)
        self.__wait_for_job_submitted(id2)
        self.__wait_for_job_submitted(id3)

        min_creation_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=1)
        job_ids = self.client.relaunch_failed_jobs(job_prefix, min_creation_time)

        self.assertRegex(job_ids[0], job_prefix + "-retry-1-.*")

    def test_get_job_list_returnsOnlyQueryJobs(self):
        table_reference = TableReference(self.DATASET, self.table_reference.table_id + "_copy")
        self.GCP_BIGQUERY_CLIENT.copy_table(sources=self.table_reference, destination=table_reference)

    @retry(stop=(stop_after_delay(10)))
    def __wait_for_job_submitted(self, job_id):
        self.GCP_BIGQUERY_CLIENT.get_job(job_id).state is not None

    def __get_table_path(self):
        return "{}.{}.{}".format(ItEmsBigqueryClient.GCP_PROJECT_ID, ItEmsBigqueryClient.DATASET.dataset_id,
                                 self.test_table.table_id)

    @classmethod
    def __dataset(cls):
        dataset = Dataset(DatasetReference(cls.GCP_PROJECT_ID, "it_test_dataset"))
        dataset.default_table_expiration_ms = cls.ONE_DAY_IN_MS
        return dataset
