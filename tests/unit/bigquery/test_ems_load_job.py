from unittest import TestCase

from bigquery.ems_job_config import EmsJobConfig
from bigquery.ems_load_job import EmsLoadJob, EmsLoadState


class TestEmsLoadJob(TestCase):

    def setUp(self):
        self.load_config = EmsJobConfig()
        error_result = {"some": "error", "happened": "here"}
        self.ems_load_job = EmsLoadJob("test-job-id", self.load_config, EmsLoadState.DONE, error_result)

    def test_state(self):
        self.assertEqual(self.ems_load_job.state, EmsLoadState.DONE)

    def test_job_id(self):
        self.assertEqual(self.ems_load_job.job_id, "test-job-id")

    def test_is_failed(self):
        self.assertTrue(self.ems_load_job.is_failed)

    def test_is_not_failed(self):
        not_failed_ems_load_job = EmsLoadJob("test-job-id", self.load_config, EmsLoadState.DONE, None)

        self.assertFalse(not_failed_ems_load_job.is_failed)
