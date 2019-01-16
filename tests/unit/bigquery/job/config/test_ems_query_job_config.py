from unittest import TestCase

from bigquery.job.config.ems_job_config import EmsJobPriority, EmsWriteDisposition, EmsCreateDisposition

from bigquery.job.config.ems_query_job_config import EmsQueryJobConfig


class TestEmsQueryJobConfig(TestCase):
    def setUp(self):
        self.ems_query_job_config = EmsQueryJobConfig(priority=EmsJobPriority.INTERACTIVE,
                                                      destination_project_id="test_project",
                                                      destination_dataset="test_dataset",
                                                      destination_table="test_table",
                                                      create_disposition=EmsCreateDisposition.CREATE_IF_NEEDED,
                                                      write_disposition=EmsWriteDisposition.WRITE_APPEND,
                                                      use_query_cache=True)

    def test_destination_project_id(self):
        self.assertEqual(self.ems_query_job_config.destination_project_id, "test_project")

    def test_destination_dataset(self):
        self.assertEqual(self.ems_query_job_config.destination_dataset, "test_dataset")

    def test_create_disposition(self):
        self.assertEqual(self.ems_query_job_config.create_disposition, EmsCreateDisposition.CREATE_IF_NEEDED)

    def test_write_disposition(self):
        self.assertEqual(self.ems_query_job_config.write_disposition, EmsWriteDisposition.WRITE_APPEND)

    def test_destination_table(self):
        self.assertEqual(self.ems_query_job_config.destination_table, "test_table")

    def test_priority(self):
        self.assertEqual(self.ems_query_job_config.priority, EmsJobPriority.INTERACTIVE)

    def test_use_query_cache(self):
        self.assertEqual(self.ems_query_job_config.use_query_cache, True)
