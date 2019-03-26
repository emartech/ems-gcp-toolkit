from unittest import TestCase

from bigquery.job.config.ems_load_job_config import EmsLoadJobConfig
from bigquery.job.ems_job_state import EmsJobState
from bigquery.job.ems_load_job import EmsLoadJob


class TestEmsLoadJob(TestCase):

    def setUp(self):
        self.expected_error_result =  {"some": "error", "happened": "here"}
        self.load_config = EmsLoadJobConfig(
            schema={"fields": [{"type": "INT64", "name": "f"}]},
            source_uri_template="",
            destination_project_id="dummy-project-id",
            destination_dataset="dummy-dataset",
            destination_table="dummy-project-id.dummy-dataset.dummy-table"
        )
        self.ems_load_job = EmsLoadJob("test-job-id", self.load_config, EmsJobState.DONE, self.expected_error_result)

    def test_state(self):
        self.assertEqual(self.ems_load_job.state, EmsJobState.DONE)

    def test_job_id(self):
        self.assertEqual(self.ems_load_job.job_id, "test-job-id")

    def test_is_failed(self):
        self.assertTrue(self.ems_load_job.is_failed)

    def test_is_not_failed(self):
        not_failed_ems_load_job = EmsLoadJob("test-job-id", self.load_config, EmsJobState.DONE, None)

        self.assertFalse(not_failed_ems_load_job.is_failed)

    def test_error_result(self):
        self.assertEqual(self.ems_load_job.error_result, self.expected_error_result)
