import random
import time
from unittest import TestCase

from google.api_core.exceptions import NotFound
from google.cloud import storage

from pubsub.ems_publisher_client import EmsPublisherClient
from storage.ems_storage_client import EmsStorageClient
from tests.integration import GCP_PROJECT_ID

IT_TEST_BUCKET = "it_test_ems_gcp_toolkit"

TOOLKIT_CREATED_BUCKET = "it_test_ems_gcp_toolkit_created_bucket"

TOOLKIT_CREATED_TOPIC = "it_test_ems_gcp_toolkit_created_topic"


class ItEmsStorageClientTest(TestCase):

    def setUp(self):
        self.__ems_storage_client = EmsStorageClient(GCP_PROJECT_ID)
        self.__ems_publisher_client = EmsPublisherClient()

    @classmethod
    def setUpClass(cls):
        cls.__storage_client = storage.Client(GCP_PROJECT_ID)
        bucket = cls.__storage_client.bucket(IT_TEST_BUCKET)
        if not bucket.exists():
            bucket.location = "europe-west1"
            bucket.storage_class = "REGIONAL"
            bucket.create()

        cls.bucket = bucket

    @classmethod
    def tearDownClass(cls):
        bucket_name = TOOLKIT_CREATED_BUCKET
        bucket = cls.__storage_client.bucket(bucket_name)
        if bucket.exists():
            bucket.delete(force=True)

    def test_download_lines_downloadingSingleLine_returnsHeader(self):
        blob_name = "sample_test_with_header.csv"
        blob = self.bucket.blob(blob_name)
        num_cols = random.randint(1, 5)
        header = ",".join(["header"] * num_cols)
        blob.upload_from_string(f"{header}\nROW\n")

        gcs_header = self.__ems_storage_client.download_lines(self.bucket.name, blob_name, 1)

        self.assertEqual([header], gcs_header)

    def test_download_lines_downloadingMultipleLines_returnsRows(self):
        blob_name = "sample_multiline.txt"
        blob = self.bucket.blob(blob_name)
        blob.upload_from_string("line1\nline2\nline3\n")

        lines = self.__ems_storage_client.download_lines(self.bucket.name, blob_name, 2)

        self.assertEqual(["line1", "line2"], lines)

    def test_download_lines_ifReturnedLinesNotEqualsRequestedLines_raiseException(self):
        blob_name = "sample_big_multiline.txt"
        blob = self.bucket.blob(blob_name)
        lines = ["line"] * 10
        blob.upload_from_string("\n".join(lines))

        with self.assertRaises(NotImplementedError):
            self.__ems_storage_client.download_lines(self.bucket.name, blob_name, len(lines), 10)

    def test_upload_from_string(self):
        blob_name = "test_upload.txt"
        content = "Test data to upload"
        self.__ems_storage_client.upload_from_string(self.bucket.name, blob_name, content)

        blob = self.bucket.blob(blob_name)
        actual_content = blob.download_as_string().decode("utf-8")
        self.assertEqual(actual_content, content)

    def test_create_bucket_if_not_exists(self):
        self.__ems_storage_client.create_bucket_if_not_exists(TOOLKIT_CREATED_BUCKET, project=GCP_PROJECT_ID,
                                                              location="europe-west1")

        bucket = self.__storage_client.bucket(TOOLKIT_CREATED_BUCKET)
        self.assertTrue(bucket.exists())

    def test_create_bucket_if_not_exists_doesNothingIfExists(self):
        self.bucket.blob("create_bucket_test_blob.txt").upload_from_string("Test data")
        self.__ems_storage_client.create_bucket_if_not_exists(IT_TEST_BUCKET, project=GCP_PROJECT_ID,
                                                              location="europe-west1")

        bucket = self.__storage_client.bucket(IT_TEST_BUCKET)
        self.assertTrue(bucket.exists())
        self.bucket.blob("create_bucket_test_blob.txt").exists()

    def test_delete_bucket_if_exists_DeletesEmptyBucket(self):
        bucket_name = self.__generate_test_name("bucket")
        self.__ems_storage_client.create_bucket_if_not_exists(bucket_name, GCP_PROJECT_ID, "europe-west1")

        self.__ems_storage_client.delete_bucket_if_exists(bucket_name)

        bucket = self.__storage_client.bucket(bucket_name)
        self.assertFalse(bucket.exists())

    def test_delete_bucket_if_exists_DeletesNotExistingBucket(self):
        bucket_name = self.__generate_test_name("bucket")

        try:
            self.__ems_storage_client.delete_bucket_if_exists(bucket_name)
        except NotFound:
            self.fail("Should not throw exception if bucket is not exist")

    def test_delete_bucket_if_exists_DeletesNotEmptyBucket(self):
        bucket_name = self.__generate_test_name("bucket")
        self.__ems_storage_client.create_bucket_if_not_exists(bucket_name, GCP_PROJECT_ID, "europe-west1")
        self.__ems_storage_client.upload_from_string(bucket_name, "test.txt", "ok")

        self.__ems_storage_client.delete_bucket_if_exists(bucket_name, force=True)

        bucket = self.__storage_client.bucket(bucket_name)
        self.assertFalse(bucket.exists())

    def test_delete_blob(self):
        blob_name = "delete_blob_test_subject.txt"
        self.bucket.blob(blob_name).upload_from_string("foo")
        self.__ems_storage_client.delete_blob(IT_TEST_BUCKET, blob_name)
        self.assertFalse(self.bucket.blob(blob_name).exists())

    def test_create_notification_if_not_exists_noNotificationExistsCreatesIt(self):
        topic_name, bucket_name = self.__setup_notification_test_dependencies()

        self.__ems_storage_client.create_notification_if_not_exists(topic_name, bucket_name)

        notification_list = self.__storage_client.bucket(bucket_name).list_notifications()
        self.assertEqual(1, len(list(notification_list)))

        self.__cleanup([topic_name], bucket_name)

    def test_create_notification_if_not_exists_notificationExistsDoNotCreateIt(self):
        topic_name, bucket_name = self.__setup_notification_test_dependencies()

        self.__ems_storage_client.create_notification_if_not_exists(topic_name, bucket_name)
        self.__ems_storage_client.create_notification_if_not_exists(topic_name, bucket_name)

        notification_list = self.__storage_client.bucket(bucket_name).list_notifications()
        self.assertEqual(1, len(list(notification_list)))

        self.__cleanup([topic_name], bucket_name)

    def test_create_notification_if_not_exists_OtherNotificationExistsCreatesIt(self):
        topic_name, bucket_name = self.__setup_notification_test_dependencies()
        topic_name_other = topic_name + "_other"
        self.__ems_publisher_client.topic_create_if_not_exists(GCP_PROJECT_ID, topic_name_other)
        self.__ems_storage_client.create_notification_if_not_exists(topic_name_other, bucket_name)

        self.__ems_storage_client.create_notification_if_not_exists(topic_name, bucket_name)

        notification_list = self.__storage_client.bucket(bucket_name).list_notifications()
        self.assertEqual(2, len(list(notification_list)))

        self.__cleanup([topic_name, topic_name_other], bucket_name)

    def test_delete_notification_if_exists(self):
        topic_name, bucket_name = self.__setup_notification_test_dependencies()
        self.__ems_storage_client.create_notification_if_not_exists(topic_name, bucket_name)

        self.__ems_storage_client.delete_notification_if_exists(topic_name, bucket_name)

        notification_list = self.__storage_client.bucket(bucket_name).list_notifications()
        self.assertEqual(0, len(list(notification_list)))

        self.__cleanup([topic_name], bucket_name)

    def test_delete_notification_if_exists_WontFailWhenNotificationWasNotCreated(self):
        topic_name, bucket_name = self.__setup_notification_test_dependencies()

        self.__ems_storage_client.delete_notification_if_exists(topic_name, bucket_name)

        notification_list = self.__storage_client.bucket(bucket_name).list_notifications()
        self.assertEqual(0, len(list(notification_list)))

        self.__cleanup([topic_name], bucket_name)

    def test_delete_notification_if_exists_DeletesOnlyOneOfTheNotifications(self):
        topic_name, bucket_name = self.__setup_notification_test_dependencies()
        topic_name_other = topic_name + "_other"
        self.__ems_publisher_client.topic_create_if_not_exists(GCP_PROJECT_ID, topic_name_other)
        self.__ems_storage_client.create_notification_if_not_exists(topic_name, bucket_name)
        self.__ems_storage_client.create_notification_if_not_exists(topic_name_other, bucket_name)

        self.__ems_storage_client.delete_notification_if_exists(topic_name, bucket_name)

        notification_list = self.__storage_client.bucket(bucket_name).list_notifications()
        self.assertEqual(1, len(list(notification_list)))

        self.__cleanup([topic_name, topic_name_other], bucket_name)

    def __setup_notification_test_dependencies(self):
        topic_name = self.__generate_test_name("topic")
        bucket_name = self.__generate_test_name("bucket")
        self.__ems_publisher_client.topic_create_if_not_exists(GCP_PROJECT_ID, topic_name)
        self.__ems_storage_client.create_bucket_if_not_exists(bucket_name, GCP_PROJECT_ID, "europe-west1")
        return topic_name, bucket_name

    def __cleanup(self, topic_names, bucket_name):
        for topic_name in topic_names:
            self.__ems_publisher_client.delete_topic_if_exists(GCP_PROJECT_ID, topic_name)
        bucket = self.__storage_client.bucket(bucket_name)
        if bucket.exists():
            bucket.delete(force=True)

    @staticmethod
    def __generate_test_name(context: str):
        return "test_" + context + "_" + str(int(time.time()))
