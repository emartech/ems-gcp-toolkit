from unittest import TestCase

from bigquery.ems_job_state import EmsJobState
from bigquery.ems_query_job import EmsQueryJob
from bigquery.ems_query_job_config import EmsQueryJobConfig


class TestEmsQueryJob(TestCase):

    def setUp(self):
        self.query_config = EmsQueryJobConfig()
        error_result = {"some": "error", "happened": "here"}
        self.ems_query_job = EmsQueryJob("test-job-id", "query", self.query_config, EmsJobState.DONE, error_result)

    def test_state(self):
        self.assertEqual(self.ems_query_job.state, EmsJobState.DONE)

    def test_job_id(self):
        self.assertEqual(self.ems_query_job.job_id, "test-job-id")

    def test_query(self):
        self.assertEqual(self.ems_query_job.query, "query")

    def test_is_failed(self):
        self.assertTrue(self.ems_query_job.is_failed)

    def test_is_not_failed(self):
        not_failed_ems_query_job = EmsQueryJob("test-job-id", "query", self.query_config, EmsJobState.DONE, None)

        self.assertFalse(not_failed_ems_query_job.is_failed)
