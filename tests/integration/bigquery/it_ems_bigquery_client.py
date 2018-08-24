import os
from unittest import TestCase

import uuid as uuid

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud.bigquery import Dataset, DatasetReference, Table, TableReference, SchemaField

from bigquery.ems_api_error import EmsApiError
from bigquery.ems_bigquery_client import EmsBigqueryClient
from bigquery.ems_query_config import EmsQueryConfig


class ItEmsBigqueryClient(TestCase):
    ONE_DAY_IN_MS = 3600000 * 24
    GCP_BIGQUERY_CLIENT = None
    DATASET = None
    GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
    DUMMY_QUERY = "SELECT 1 AS data"
    INSERT_TEMPLATE = "INSERT INTO `{}` (int_data, str_data) VALUES (1, 'hello')"
    SELECT_TEMPLATE = "SELECT * FROM `{}`"
    DUMMY_SELECT_TO_TABLE = "SELECT 1 AS int_data, 'hello' AS str_data"

    # TODO fix test dataset, temp table for each test case
    @classmethod
    def setUpClass(cls):
        cls.GCP_BIGQUERY_CLIENT = bigquery.Client(cls.GCP_PROJECT_ID, location="EU")
        cls.DATASET = cls.__create_test_dataset()
        cls.GCP_BIGQUERY_CLIENT.create_dataset(cls.DATASET)

    @classmethod
    def tearDownClass(cls):
        cls.GCP_BIGQUERY_CLIENT.delete_dataset(cls.DATASET, True)

    def setUp(self):
        table_reference = TableReference(self.DATASET.reference, "test_table")
        self.test_table = Table(table_reference, [SchemaField("int_data", "INT64"), SchemaField("str_data", "STRING")])
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
        ems_query_config = EmsQueryConfig(
            destination_dataset=ItEmsBigqueryClient.DATASET.dataset_id,
            destination_table=self.test_table.table_id
        )
        query_with_destination_result = list(self.client.run_sync_query(self.DUMMY_SELECT_TO_TABLE,
                                                                        ems_query_config=ems_query_config))
        query_result = list(self.client.run_sync_query(self.SELECT_TEMPLATE.format(self.__get_table_path())))

        assert [{"int_data": 1, "str_data": "hello"}] == query_result
        assert query_with_destination_result == query_result

    def test_run_async_query_submitsJob(self):
        job_id = self.client.run_async_query(self.DUMMY_QUERY)

        job = self.GCP_BIGQUERY_CLIENT.get_job(job_id)

        assert job.state is not None

    def __get_table_path(self):
        return "{}.{}.{}".format(ItEmsBigqueryClient.GCP_PROJECT_ID, ItEmsBigqueryClient.DATASET.dataset_id,
                                 self.test_table.table_id)

    @classmethod
    def __create_test_dataset(cls):
        dataset = Dataset(DatasetReference(cls.GCP_PROJECT_ID, cls.__generate_unique_id()))
        dataset.default_table_expiration_ms = cls.ONE_DAY_IN_MS
        return dataset

    @staticmethod
    def __generate_unique_id():
        return "it_test_{}".format(uuid.uuid4().hex)
