from unittest import TestCase

from bigquery.ems_query_job import EmsQueryJob, EmsQueryState


class TestEmsQueryJob(TestCase):
    def setUp(self):
        self.errors = [{"some": "error", "happened": "here"}]
        self.ems_query_job = EmsQueryJob("test-job-id", "query", EmsQueryState.DONE, self.errors)

    def test_errors(self):
        self.assertEqual(self.ems_query_job.errors, self.errors)

    def test_state(self):
        self.assertEqual(self.ems_query_job.state, EmsQueryState.DONE)

    def test_job_id(self):
        self.assertEqual(self.ems_query_job.job_id, "test-job-id")

    def test_query(self):
        self.assertEqual(self.ems_query_job.query, "query")

    def test_is_failed(self):
        self.assertTrue(self.ems_query_job.is_failed)

    def test_is_not_failed(self):
        not_failed_ems_query_job = EmsQueryJob("test-job-id", "query", EmsQueryState.DONE, None)

        self.assertFalse(not_failed_ems_query_job.is_failed)
