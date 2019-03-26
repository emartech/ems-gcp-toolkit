from unittest import TestCase

from bigquery.job.config.ems_query_job_config import EmsQueryJobConfig
from bigquery.job.ems_job_state import EmsJobState
from bigquery.job.ems_query_job import EmsQueryJob


class TestEmsQueryJob(TestCase):


    def setUp(self):
        self.query_config = EmsQueryJobConfig()
        self.expected_error_result = {"some": "error", "happened": "here"}
        self.ems_query_job = EmsQueryJob("test-job-id", "query", self.query_config, EmsJobState.DONE,  self.expected_error_result)

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

    def test_error_result(self):
        self.assertEqual(self.ems_query_job.error_result,  self.expected_error_result)
