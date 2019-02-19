import random
from unittest import TestCase

from google.cloud import storage

from storage.ems_storage_client import EmsStorageClient
from tests.integration import GCP_PROJECT_ID


class ItEmsStorageClientTest(TestCase):

    @classmethod
    def setUpClass(cls):
        bucket_name = "it_test_ems_gcp_toolkit"
        storage_client = storage.Client(GCP_PROJECT_ID)
        bucket = storage_client.bucket(bucket_name)
        if not bucket.exists():
            bucket.location = "europe-west1"
            bucket.storage_class = "REGIONAL"
            bucket.create()

        cls.bucket = bucket

    def test_download_lines_downloadingSingleLine_returnsHeader(self):
        blob_name = "sample_test_with_header.csv"
        blob = self.bucket.blob(blob_name)
        num_cols = random.randint(1,5)
        header = ",".join(["header"]*num_cols)
        blob.upload_from_string(f"{header}\nROW\n")

        ems_client = EmsStorageClient(GCP_PROJECT_ID)
        gcs_header = ems_client.download_lines(self.bucket.name, blob_name, 1)

        self.assertEqual([header], gcs_header)

    def test_download_lines_downloadingMultipleLines_returnsRows(self):
        blob_name = "sample_multiline.txt"
        blob = self.bucket.blob(blob_name)
        blob.upload_from_string("line1\nline2\nline3\n")

        ems_client = EmsStorageClient(GCP_PROJECT_ID)
        lines = ems_client.download_lines(self.bucket.name, blob_name, 2)

        self.assertEqual(["line1", "line2"], lines)

    def test_download_lines_ifReturnedLinesNotEqualsRequestedLines_raiseException(self):
        blob_name = "sample_big_multiline.txt"
        blob = self.bucket.blob(blob_name)
        lines = ["line"] * 10
        blob.upload_from_string("\n".join(lines))

        ems_client = EmsStorageClient(GCP_PROJECT_ID)
        with self.assertRaises(NotImplementedError):
            ems_client.download_lines(self.bucket.name, blob_name, len(lines), 10)
