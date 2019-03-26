from unittest import TestCase

from bigquery.job.ems_extract_job import EmsExtractJob
from bigquery.job.ems_job_state import EmsJobState


class TestEmsExtractJob(TestCase):

    def setUp(self):
        self.expected_error_result = {"some": "error", "happened": "here"}
        self.ems_extract_job = EmsExtractJob("test-job-id", "dummy-project-id.dummy-dataset.dummy-table",
                                             ["gs://some-non-existing-bucket-id/destination1"], EmsJobState.DONE,
                                             self.expected_error_result)

    def test_state(self):
        self.assertEqual(self.ems_extract_job.state, EmsJobState.DONE)

    def test_job_id(self):
        self.assertEqual(self.ems_extract_job.job_id, "test-job-id")

    def test_is_failed(self):
        self.assertTrue(self.ems_extract_job.is_failed)

    def test_is_not_failed(self):
        not_failed_ems_load_job = EmsExtractJob("test-job-id", "dummy-project-id.dummy-dataset.dummy-table",
                                             ["gs://some-non-existing-bucket-id/destination1"], EmsJobState.DONE, None)

        self.assertFalse(not_failed_ems_load_job.is_failed)

    def test_error_result(self):
        self.assertEqual(self.ems_extract_job.error_result, self.expected_error_result)
