from unittest import TestCase
from unittest.mock import patch, Mock

from storage.ems_storage_client import EmsStorageClient


class TestEmsStorageClient(TestCase):

    @patch("storage.ems_storage_client.storage")
    def test_download_lines_handlesLineEndingsProperly(self, storage_module):
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        storage_module.Client.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        blob_content = "ok" + chr(456789)
        harmed_encoded_blob_content_with_partial_bytes_on_end = blob_content.encode("utf-8")[0:5]
        mock_blob.download_as_string.return_value = harmed_encoded_blob_content_with_partial_bytes_on_end

        ems_client = EmsStorageClient("some-project-id")
        result_lines = ems_client.download_lines("some-bucket-name", "some-blob", num_lines=1, chunk_size=5)

        self.assertEqual(result_lines[0], "ok")
