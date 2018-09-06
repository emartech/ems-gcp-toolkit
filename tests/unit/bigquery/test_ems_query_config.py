from unittest import TestCase

from bigquery.ems_query_config import EmsQueryConfig, EmsQueryPriority, EmsCreateDisposition, EmsWriteDisposition


class TestEmsQueryConfig(TestCase):

    def setUp(self):
        self.ems_query_config = EmsQueryConfig(priority=EmsQueryPriority.INTERACTIVE,
                                               destination_dataset="test_dataset",
                                               destination_table="test_table",
                                               create_disposition=EmsCreateDisposition.CREATE_IF_NEEDED,
                                               write_disposition=EmsWriteDisposition.WRITE_APPEND)

    def test_destination_dataset(self):
        self.assertEqual(self.ems_query_config.destination_dataset, "test_dataset")

    def test_create_disposition(self):
        self.assertEqual(self.ems_query_config.create_disposition, EmsCreateDisposition.CREATE_IF_NEEDED)

    def test_write_disposition(self):
        self.assertEqual(self.ems_query_config.write_disposition, EmsWriteDisposition.WRITE_APPEND)

    def test_destination_table(self):
        self.assertEqual(self.ems_query_config.destination_table, "test_table")

    def test_priority(self):
        self.assertEqual(self.ems_query_config.priority, EmsQueryPriority.INTERACTIVE)
