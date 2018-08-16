import os
from unittest import TestCase

import uuid as uuid
from google.cloud import bigquery
from google.cloud.bigquery import Dataset, DatasetReference, Table, TableReference, SchemaField

from bigquery.ems_api_error import EmsApiError
from bigquery.ems_bigquery_client import EmsBigqueryClient


class ItEmsBigqueryClient(TestCase):
    GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
    GCP_BIGQUERY_CLIENT = bigquery.Client(GCP_PROJECT_ID, location="EU")
    DATASET = None
    TABLE = None

    DUMMY_QUERY = "SELECT 1 AS data"
    INSERT_TEMPLATE = "INSERT INTO `{}` (int_data, str_data) VALUES (1, 'hello')"
    SELECT_TEMPLATE = "SELECT * FROM `{}`"

    @classmethod
    def setUpClass(cls):
        cls.DATASET = cls.create_test_dataset()
        cls.GCP_BIGQUERY_CLIENT.create_dataset(cls.DATASET)
        table_reference = TableReference(cls.DATASET.reference, "test_table")
        cls.TABLE = Table(table_reference, [SchemaField("int_data", "INT64"), SchemaField("str_data", "STRING")])
        cls.GCP_BIGQUERY_CLIENT.create_table(cls.TABLE)

    @classmethod
    def tearDownClass(cls):
        cls.GCP_BIGQUERY_CLIENT.delete_dataset(cls.DATASET, True)

    def setUp(self):
        self.client = EmsBigqueryClient(self.GCP_PROJECT_ID)

    def test_run_sync_query_dummyQuery(self):
        result = self.client.run_sync_query(self.DUMMY_QUERY)

        rows = list(result)
        assert 1 == len(rows)
        assert {"data": 1} == rows[0]

    def test_run_sync_query_nonExistingDataset(self):
        result = self.client.run_sync_query("SELECT * FROM `non_existing_dataset.whatever`")

        with self.assertRaises(EmsApiError) as context:
            list(result)

        error_message = context.exception.args[0].lower()
        assert "not found" in error_message
        assert self.GCP_PROJECT_ID in error_message
        assert "non_existing_dataset" in error_message

    def test_run_sync_query_onExistingData(self):
        query = self.INSERT_TEMPLATE.format(self.get_table_path())
        insert_result = self.client.run_sync_query(query)
        list(insert_result)

        query_result = self.client.run_sync_query(self.SELECT_TEMPLATE.format(self.get_table_path()))

        assert [{"int_data": 1, "str_data": "hello"}] == list(query_result)

    def test_run_async_query_submitsJob(self):
        job_id = self.client.run_async_query(self.DUMMY_QUERY)

        job = self.GCP_BIGQUERY_CLIENT.get_job(job_id)

        assert job.state is not None

    @classmethod
    def get_table_path(cls):
        return "{}.{}.{}".format(cls.GCP_PROJECT_ID, cls.DATASET.dataset_id, cls.TABLE.table_id)

    @classmethod
    def create_test_dataset(cls):
        return Dataset(DatasetReference(cls.GCP_PROJECT_ID, cls.generate_unique_id()))

    @staticmethod
    def generate_unique_id():
        return "it_test_{}".format(uuid.uuid4().hex)
