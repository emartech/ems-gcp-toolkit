import random
from unittest import TestCase

from google.cloud import storage

from pubsub.ems_publisher_client import EmsPublisherClient
from storage.ems_storage_client import EmsStorageClient
from tests.integration import GCP_PROJECT_ID

IT_TEST_BUCKET = "it_test_ems_gcp_toolkit"

TOOLKIT_CREATED_BUCKET = "it_test_ems_gcp_toolkit_created_bucket"

TOOLKIT_CREATED_TOPIC = "it_test_ems_gcp_toolkit_created_topic"


class ItEmsStorageClientTest(TestCase):

    def setUp(self):
        self.ems_storage_client = EmsStorageClient(GCP_PROJECT_ID)
        self.ems_publisher_client = EmsPublisherClient()

    @classmethod
    def setUpClass(cls):
        cls.storage_client = storage.Client(GCP_PROJECT_ID)
        bucket = cls.storage_client.bucket(IT_TEST_BUCKET)
        if not bucket.exists():
            bucket.location = "europe-west1"
            bucket.storage_class = "REGIONAL"
            bucket.create()

        cls.bucket = bucket

    @classmethod
    def tearDownClass(cls):
        bucket_name = TOOLKIT_CREATED_BUCKET
        bucket = cls.storage_client.bucket(bucket_name)
        if bucket.exists():
            bucket.delete(force=True)

    def test_download_lines_downloadingSingleLine_returnsHeader(self):
        blob_name = "sample_test_with_header.csv"
        blob = self.bucket.blob(blob_name)
        num_cols = random.randint(1, 5)
        header = ",".join(["header"] * num_cols)
        blob.upload_from_string(f"{header}\nROW\n")

        gcs_header = self.ems_storage_client.download_lines(self.bucket.name, blob_name, 1)

        self.assertEqual([header], gcs_header)

    def test_download_lines_downloadingMultipleLines_returnsRows(self):
        blob_name = "sample_multiline.txt"
        blob = self.bucket.blob(blob_name)
        blob.upload_from_string("line1\nline2\nline3\n")

        lines = self.ems_storage_client.download_lines(self.bucket.name, blob_name, 2)

        self.assertEqual(["line1", "line2"], lines)

    def test_download_lines_ifReturnedLinesNotEqualsRequestedLines_raiseException(self):
        blob_name = "sample_big_multiline.txt"
        blob = self.bucket.blob(blob_name)
        lines = ["line"] * 10
        blob.upload_from_string("\n".join(lines))

        with self.assertRaises(NotImplementedError):
            self.ems_storage_client.download_lines(self.bucket.name, blob_name, len(lines), 10)

    def test_upload_from_string(self):
        blob_name = "test_upload.txt"
        content = "Test data to upload"
        self.ems_storage_client.upload_from_string(self.bucket.name, blob_name, content)

        blob = self.bucket.blob(blob_name)
        actual_content = blob.download_as_string().decode("utf-8")
        self.assertEqual(actual_content, content)

    def test_create_bucket_if_not_exists(self):
        self.ems_storage_client.create_bucket_if_not_exists(TOOLKIT_CREATED_BUCKET, project=GCP_PROJECT_ID,
                                                            location="europe-west1")

        bucket = self.storage_client.bucket(TOOLKIT_CREATED_BUCKET)
        self.assertTrue(bucket.exists())

    def test_create_bucket_if_not_exists_doesNothingIfExists(self):
        self.bucket.blob("create_bucket_test_blob.txt").upload_from_string("Test data")
        self.ems_storage_client.create_bucket_if_not_exists(IT_TEST_BUCKET, project=GCP_PROJECT_ID,
                                                            location="europe-west1")

        bucket = self.storage_client.bucket(IT_TEST_BUCKET)
        self.assertTrue(bucket.exists())
        self.bucket.blob("create_bucket_test_blob.txt").exists()

    def test_delete_blob(self):
        blob_name = "delete_blob_test_subject.txt"
        self.bucket.blob(blob_name).upload_from_string("foo")
        self.ems_storage_client.delete_blob(IT_TEST_BUCKET, blob_name)
        self.assertFalse(self.bucket.blob(blob_name).exists())

    def test_create_notification_if_not_exists_noNotificationExistsCreatesIt(self):
        self.setupNotificationDependencies()

        self.ems_storage_client.create_notification_if_not_exists(TOOLKIT_CREATED_TOPIC, TOOLKIT_CREATED_BUCKET)

        self.assertNotificationListCount(1)

    def test_create_notification_if_not_exists_notificationExistsDoNotCreateIt(self):
        self.setupNotificationDependencies()

        self.ems_storage_client.create_notification_if_not_exists(TOOLKIT_CREATED_TOPIC, TOOLKIT_CREATED_BUCKET)
        self.ems_storage_client.create_notification_if_not_exists(TOOLKIT_CREATED_TOPIC, TOOLKIT_CREATED_BUCKET)

        self.assertNotificationListCount(1)

    def assertNotificationListCount(self, count: int):
        result_notification_list = []

        for notification_item in self.storage_client.bucket(TOOLKIT_CREATED_BUCKET).list_notifications():
            result_notification_list.append(notification_item)
        self.assertEqual(count, len(result_notification_list))

    def setupNotificationDependencies(self):
        self.ems_publisher_client.topic_create_if_not_exists(GCP_PROJECT_ID, TOOLKIT_CREATED_TOPIC)
        self.ems_storage_client.create_bucket_if_not_exists(TOOLKIT_CREATED_BUCKET)
        self.cleanup_test_notifications()

    def cleanup_test_notifications(self):
        for notification in self.storage_client.bucket(TOOLKIT_CREATED_BUCKET).list_notifications():
            notification.delete()
