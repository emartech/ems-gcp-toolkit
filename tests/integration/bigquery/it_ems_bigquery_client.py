import datetime
import random
import time
import uuid
from unittest import TestCase

from google.api_core.exceptions import NotFound, Conflict
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery import Dataset, DatasetReference, Table, TableReference, SchemaField, TimePartitioning
from tenacity import retry, stop_after_delay, retry_if_result

from bigquery.ems_api_error import EmsApiError
from bigquery.ems_bigquery_client import EmsBigqueryClient
from bigquery.job.config.ems_job_config import EmsWriteDisposition
from bigquery.job.config.ems_load_job_config import EmsLoadJobConfig
from bigquery.job.config.ems_query_job_config import EmsQueryJobConfig
from bigquery.job.ems_extract_job import EmsExtractJob
from bigquery.job.ems_job_state import EmsJobState
from bigquery.job.ems_load_job import EmsLoadJob
from bigquery.job.ems_query_job import EmsQueryJob
from tests.integration import GCP_PROJECT_ID


class ItEmsBigqueryClient(TestCase):
    ONE_DAY_IN_MS = 3600000 * 24
    GCP_BIGQUERY_CLIENT = None
    DATASET = None
    DUMMY_QUERY = "SELECT 1 AS data"
    BAD_QUERY = "VERY BAD QUERY"
    INSERT_TEMPLATE = "INSERT INTO `{}` (int_data, str_data) VALUES (1, 'hello')"
    SELECT_TEMPLATE = "SELECT * FROM `{}`"
    DUMMY_SELECT_TO_TABLE = "SELECT 1 AS int_data, 'hello' AS str_data"

    @classmethod
    def setUpClass(cls):
        cls.GCP_BIGQUERY_CLIENT = bigquery.Client(GCP_PROJECT_ID, location="EU")
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
        self.test_table  = self.__create_test_table(table_name, self.DATASET.reference)
        self.client = EmsBigqueryClient(GCP_PROJECT_ID)
        self.storage_client = storage.Client(GCP_PROJECT_ID)

    def __create_test_table(self, table_name, dataset_id):
        table_schema = [SchemaField("int_data", "INT64"), SchemaField("str_data", "STRING")]
        table_reference = TableReference(dataset_id, table_name)
        test_table = Table(table_reference, table_schema)
        test_table.time_partitioning = TimePartitioning("DAY")
        self.__delete_if_exists(test_table)
        self.GCP_BIGQUERY_CLIENT.create_table(test_table)
        return test_table

    def __get_test_bucket(self, bucket_name):

        try:
            bucket = self.storage_client.get_bucket(bucket_name)
        except NotFound:
            bucket = self.storage_client.bucket(bucket_name)
            bucket.location = "europe-west1"
            bucket.storage_class = "REGIONAL"
            bucket.create()
        return bucket

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
        assert GCP_PROJECT_ID in error_message
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

    def test_run_get_job_list_returnsQueryJob(self):
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

    def test_get_job_list_returnsLoadJob(self):
        config = EmsLoadJobConfig({"fields": [{"name": "some_name", "type": "STRING"}]},
                                  "gs://some-non-existing-bucket-id/blob-id",
                                  destination_project_id=GCP_PROJECT_ID,
                                  destination_dataset="it_test_dataset",
                                  destination_table="some_table")
        min_creation_time = datetime.datetime.utcnow()
        unique_id = self.client.run_async_load_job("load_job_test", config)
        self.__wait_for_job_done(unique_id)
        jobs_iterator = self.client.get_jobs_with_prefix("load_job_test", min_creation_time)
        found = unique_id in [job.job_id for job in jobs_iterator]

        self.assertTrue(found)

    def test_get_job_list_returnsExtractJob(self):
        min_creation_time = datetime.datetime.utcnow()
        destination_uris = ["gs://some-non-existing-bucket-id/destination1"]
        table_path = self.__get_table_path()
        unique_id = self.client.run_async_extract_job("extract_job_test", table_path, destination_uris)
        self.__wait_for_job_done(unique_id)
        jobs_iterator = self.client.get_jobs_with_prefix("extract_job_test", min_creation_time)

        job: EmsExtractJob = next(j for j in jobs_iterator if j.job_id == unique_id)

        self.assertEqual(job.table, table_path)
        self.assertEqual(job.destination_uris, destination_uris)
        self.assertIsInstance(job.state, EmsJobState)
        self.assertEqual(job.state.value, "DONE")
        self.assertTrue(job.is_failed)

    def test_run_async_extract_job_shouldSaveToBucket(self):
        query = self.INSERT_TEMPLATE.format(self.__get_table_path())
        self.client.run_sync_query(query)
        min_creation_time = datetime.datetime.utcnow()

        bucket_name = GCP_PROJECT_ID + "-gcp-toolkit-it"
        bucket = self.__get_test_bucket(bucket_name)
        blob_name = f'exported_{int(min_creation_time.timestamp())}.csv'

        job = self.__run_async_extract_job(min_creation_time, bucket_name, blob_name, False)

        blob = bucket.blob(blob_name)
        self.assertFalse(job.is_failed)
        self.assertTrue(blob.exists())
        self.assertEqual(blob.download_as_string(), b'1,hello\n')

        bucket.delete_blob(blob_name)

    def test_run_async_extract_job_shouldSaveToBucketWithHeader(self):
        query = self.INSERT_TEMPLATE.format(self.__get_table_path())
        self.client.run_sync_query(query)
        min_creation_time = datetime.datetime.utcnow()

        bucket_name = GCP_PROJECT_ID + "-gcp-toolkit-it"
        bucket = self.__get_test_bucket(bucket_name)
        blob_name = f'exported_{int(min_creation_time.timestamp())}.csv'

        job = self.__run_async_extract_job(min_creation_time, bucket_name, blob_name, True)

        blob = bucket.blob(blob_name)
        self.assertFalse(job.is_failed)
        self.assertTrue(blob.exists())
        self.assertEqual(blob.download_as_string(), b'int_data,str_data\n1,hello\n')

        bucket.delete_blob(blob_name)

    def __run_async_extract_job(self, min_creation_time, bucket_name, blob_name, print_header):
        table_path = self.__get_table_path()
        job_id_prefix = "extract_job_test"
        unique_id = self.client.run_async_extract_job(job_id_prefix, table_path, [f'gs://{bucket_name}/{blob_name}'],
                                                      print_header)
        self.__wait_for_job_done(unique_id)
        jobs_iterator = self.client.get_jobs_with_prefix(job_id_prefix, min_creation_time)
        job: EmsExtractJob = next(j for j in jobs_iterator if j.job_id == unique_id)
        return job

    def test_get_job_list_returnsAllKindOfJobs(self):
        load_config = EmsLoadJobConfig({"fields": [{"name": "some_name", "type": "STRING"}]},
                                       "gs://some-non-existing-bucket-id/blob-id",
                                       destination_project_id=GCP_PROJECT_ID,
                                       destination_dataset="it_test_dataset",
                                       destination_table="some_table")
        destination_uris = ["gs://some-non-existing-bucket-id/destination1"]

        min_creation_time = datetime.datetime.utcnow()
        id_for_query_job = self.client.run_async_query(self.DUMMY_QUERY, job_id_prefix="it_job")
        id_for_load_job = self.client.run_async_load_job(job_id_prefix="it_job", config=load_config)
        id_for_extract_job = self.client.run_async_extract_job(job_id_prefix="it_job", table=self.__get_table_path(),
                                                               destination_uris=destination_uris)

        self.__wait_for_job_done(id_for_query_job)
        self.__wait_for_job_done(id_for_load_job)
        self.__wait_for_job_done(id_for_extract_job)
        jobs_iterator = self.client.get_jobs_with_prefix("it_job", min_creation_time)
        job_types = [type(j) for j in jobs_iterator]

        self.assertEqual(3, len(job_types))
        self.assertIn(EmsQueryJob, job_types)
        self.assertIn(EmsLoadJob, job_types)
        self.assertIn(EmsExtractJob, job_types)

    def test_run_async_load_job_loadsFileFromBucketToNewBigqueryTable(self):
        bucket_name = "it_test_ems_gcp_toolkit"
        bucket = self.__get_test_bucket(bucket_name)
        blob_name = "sample_fruit_test.csv"
        blob = bucket.blob(blob_name)
        random_quantity = random.randint(10000, 99000)
        blob.upload_from_string(f"apple,{random_quantity},True,1970-01-01T12:00:00.000Z\n")
        source_uri = f"gs://{bucket_name}/{blob_name}"
        config = EmsLoadJobConfig(source_uri_template=source_uri,
                                  destination_project_id=GCP_PROJECT_ID,
                                  destination_dataset=self.DATASET.dataset_id,
                                  destination_table="load_job_test",
                                  schema={"fields": [{"type": "STRING", "name": "fruit"},
                                                     {"type": "INT64", "name": "quantity"},
                                                     {"type": "BOOL", "name": "is_delicious"},
                                                     {"type": "TIMESTAMP", "name": "best_before"}]},
                                  write_disposition=EmsWriteDisposition.WRITE_TRUNCATE)

        load_job_id = self.client.run_async_load_job("it_test", config)
        self.__wait_for_job_done(load_job_id)

        query = f"""
        SELECT * from `{config.destination_project_id}.{config.destination_dataset}.{config.destination_table}`
        """

        result = self.client.run_sync_query(query=query)
        expected = [{"fruit": "apple", "quantity": random_quantity, "is_delicious": True,
                     "best_before": datetime.datetime(1970, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)}]
        self.assertEquals(expected, list(result))

    def test_run_async_load_job_whenLoadingFileWithHeader_headerIsSkiped(self):
        bucket_name = "it_test_ems_gcp_toolkit"
        bucket = self.__get_test_bucket(bucket_name)
        blob_name = "sample_test_with_header.csv"
        blob = bucket.blob(blob_name)
        blob.upload_from_string(f"HEADER\nROW\n")
        source_uri = f"gs://{bucket_name}/{blob_name}"
        config = EmsLoadJobConfig(source_uri_template=source_uri,
                                  destination_project_id=GCP_PROJECT_ID,
                                  destination_dataset=self.DATASET.dataset_id,
                                  destination_table="load_job_test_skip_header",
                                  schema={"fields": [{"type": "STRING", "name": "COLUMN"}]},
                                  write_disposition=EmsWriteDisposition.WRITE_TRUNCATE,
                                  skip_leading_rows=1)

        load_job_id = self.client.run_async_load_job("it_test", config)
        self.__wait_for_job_done(load_job_id)

        query = f"""
        SELECT * from `{config.destination_project_id}.{config.destination_dataset}.{config.destination_table}`
        """

        result = self.client.run_sync_query(query=query)
        expected = [{"COLUMN": "ROW"}]
        self.assertEquals(expected, list(result))

    def test_dataset_exists_WhenDatasetNotExists(self):
        dataset_id = self.__generate_test_name("dataset")

        self.assertFalse(self.client.dataset_exists(dataset_id))

    def test_dataset_exists_WhenDatasetExists(self):
        dataset_id = self.__generate_test_name("dataset")
        self.client.create_dataset_if_not_exists(dataset_id)

        self.assertTrue(self.client.dataset_exists(dataset_id))

        self.client.delete_dataset_if_exists(dataset_id)

    def test_create_dataset_if_not_exists_CreatesDataset(self):
        dataset_id = self.__generate_test_name("dataset")

        self.client.create_dataset_if_not_exists(dataset_id)

        self.assertTrue(self.client.dataset_exists(dataset_id))

        self.client.delete_dataset_if_exists(dataset_id)

    def test_create_dataset_if_not_exists_DoesNotRaiseExceptionWhenAlreadyExists(self):
        dataset_id = self.__generate_test_name("dataset")

        self.client.create_dataset_if_not_exists(dataset_id)
        try:
            self.client.create_dataset_if_not_exists(dataset_id)
        except Conflict:
            self.fail("create_dataset_if_not_exists raised AlreadyExists error")

        self.client.delete_dataset_if_exists(dataset_id)

    def test_delete_dataset_if_exists_WhenItIsEmpty(self):
        dataset_id = self.__generate_test_name("dataset")
        self.client.create_dataset_if_not_exists(dataset_id)

        self.client.delete_dataset_if_exists(dataset_id)

        self.assertFalse(self.client.dataset_exists(dataset_id))

    def test_delete_dataset_if_exists_WhenItIsNotEmpty(self):
        dataset_id = self.__generate_test_name("dataset")
        table_name = self.__generate_test_name("table")
        self.client.create_dataset_if_not_exists(dataset_id)
        self.__create_test_table(table_name, Dataset(DatasetReference(GCP_PROJECT_ID, dataset_id)).reference)

        self.client.delete_dataset_if_exists(dataset_id, delete_contents=True)

        self.assertFalse(self.client.dataset_exists(dataset_id))

    def test_delete_dataset_DoesNotRaiseExceptionWhenNotExists(self):
        dataset_id = self.__generate_test_name("dataset")

        try:
            self.client.delete_dataset_if_exists(dataset_id)
        except NotFound:
            self.fail("delete_dataset_if_exists raised NotFound error")

    @retry(stop=(stop_after_delay(10)))
    def __wait_for_job_submitted(self, job_id):
        self.GCP_BIGQUERY_CLIENT.get_job(job_id)

    @retry(stop=(stop_after_delay(10)), retry=(retry_if_result(lambda result: result != EmsJobState.DONE.value)))
    def __wait_for_job_done(self, job_id):
        return self.GCP_BIGQUERY_CLIENT.get_job(job_id).state

    def __get_table_path(self):
        return "{}.{}.{}".format(GCP_PROJECT_ID, self.DATASET.dataset_id,
                                 self.test_table.table_id)

    @classmethod
    def __dataset(cls):
        dataset = Dataset(DatasetReference(GCP_PROJECT_ID, "it_test_dataset"))
        dataset.default_table_expiration_ms = cls.ONE_DAY_IN_MS
        return dataset

    @staticmethod
    def __generate_test_name(context: str):
        return "test_" + context + "_" + str(int(time.time()))