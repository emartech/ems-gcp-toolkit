import datetime
import os
import random
import uuid
from unittest import TestCase

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery import Dataset, DatasetReference, Table, TableReference, SchemaField, TimePartitioning
from google.cloud.exceptions import Conflict
from tenacity import retry, stop_after_delay, retry_if_result

from bigquery.ems_api_error import EmsApiError
from bigquery.ems_bigquery_client import EmsBigqueryClient
from bigquery.job.config.ems_job_config import EmsWriteDisposition
from bigquery.job.config.ems_load_job_config import EmsLoadJobConfig
from bigquery.job.config.ems_query_job_config import EmsQueryJobConfig
from bigquery.job.ems_job_state import EmsJobState


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

    def test_run_async_load_job_loadsFileFromBucketToNewBigqueryTable(self):
        storage_client = storage.Client(self.GCP_PROJECT_ID,)
        bucket_name = "it_test_ems_gcp_toolkit"
        try:
            bucket = storage_client.get_bucket(bucket_name)
        except NotFound:
            bucket = storage_client.bucket(bucket_name)
            bucket.location = "europe-west1"
            bucket.storage_class = "REGIONAL"
            bucket.create()
        blob_name = "sample_fruit_test.csv"
        blob = bucket.blob(blob_name)
        random_quantity = random.randint(10000, 99000)
        blob.upload_from_string(f"apple,{random_quantity},True,1970-01-01T12:00:00.000Z\n")
        config = EmsLoadJobConfig(destination_project_id=self.GCP_PROJECT_ID,
                                  destination_dataset=self.DATASET.dataset_id,
                                  destination_table="load_job_test",
                                  schema={"fields": [{"type": "STRING", "name": "fruit"},
                                                     {"type": "INT64", "name": "quantity"},
                                                     {"type": "BOOL", "name": "is_delicious"},
                                                     {"type": "TIMESTAMP", "name": "best_before"}]},
                                  write_disposition=EmsWriteDisposition.WRITE_TRUNCATE)

        load_job_id = self.client.run_async_load_job(f"gs://{bucket_name}/{blob_name}", "it_test", config)
        self.__wait_for_job_done(load_job_id)

        query = f"""
        SELECT * from `{config.destination_project_id}.{config.destination_dataset}.{config.destination_table}`
        """

        result = self.client.run_sync_query(query=query)
        expected = [{"fruit": "apple", "quantity": random_quantity, "is_delicious": True,
                 "best_before": datetime.datetime(1970, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)}]
        self.assertEquals(expected, list(result))

    @retry(stop=(stop_after_delay(10)))
    def __wait_for_job_submitted(self, job_id):
        self.GCP_BIGQUERY_CLIENT.get_job(job_id)

    @retry(stop=(stop_after_delay(10)), retry=(retry_if_result(lambda result: result != EmsJobState.DONE.value)))
    def __wait_for_job_done(self, job_id):
        return self.GCP_BIGQUERY_CLIENT.get_job(job_id).state

    def __get_table_path(self):
        return "{}.{}.{}".format(ItEmsBigqueryClient.GCP_PROJECT_ID, ItEmsBigqueryClient.DATASET.dataset_id,
                                 self.test_table.table_id)

    @classmethod
    def __dataset(cls):
        dataset = Dataset(DatasetReference(cls.GCP_PROJECT_ID, "it_test_dataset"))
        dataset.default_table_expiration_ms = cls.ONE_DAY_IN_MS
        return dataset
