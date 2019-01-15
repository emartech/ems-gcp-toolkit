from unittest import TestCase

from bigquery.job.config.ems_load_job_config import EmsLoadJobConfig

from bigquery.job.config.ems_job_config import EmsJobPriority, EmsCreateDisposition, EmsWriteDisposition

SCHEMA = {"fields": [{"type": "INT64", "name": "f"}]}


class TestEmsLoadJobConfig(TestCase):
    def setUp(self):
        self.ems_load_job_config = EmsLoadJobConfig(destination_project_id="test_project",
                                                    destination_dataset="test_dataset",
                                                    destination_table="test_table",
                                                    create_disposition=EmsCreateDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=EmsWriteDisposition.WRITE_APPEND,
                                                    schema=SCHEMA)

    def test_destination_project_id(self):
        self.assertEqual(self.ems_load_job_config.destination_project_id, "test_project")

    def test_destination_dataset(self):
        self.assertEqual(self.ems_load_job_config.destination_dataset, "test_dataset")

    def test_create_disposition(self):
        self.assertEqual(self.ems_load_job_config.create_disposition, EmsCreateDisposition.CREATE_IF_NEEDED)

    def test_write_disposition(self):
        self.assertEqual(self.ems_load_job_config.write_disposition, EmsWriteDisposition.WRITE_APPEND)

    def test_destination_table(self):
        self.assertEqual(self.ems_load_job_config.destination_table, "test_table")

    def test_destination_project_id_ifProjectIdIsNone_raisesValueError(self):
        load_config = EmsLoadJobConfig(destination_project_id=None, schema=SCHEMA)

        with self.assertRaises(ValueError):
            load_config.destination_project_id

    def test_destination_project_id_ifProjectIdIsEmptyString_raisesValueError(self):
        load_config = EmsLoadJobConfig(destination_project_id="", schema=SCHEMA)

        with self.assertRaises(ValueError):
            load_config.destination_project_id

    def test_destination_project_id_ifProjectIdIsMultipleWhitespaces_raisesValueError(self):
        load_config = EmsLoadJobConfig(destination_project_id="     \t  ", schema=SCHEMA)

        with self.assertRaises(ValueError):
            load_config.destination_project_id

    def test_destination_dataset_ifDatasetIsNone_raisesValueError(self):
        load_config = EmsLoadJobConfig(destination_dataset=None, schema=SCHEMA)

        with self.assertRaises(ValueError):
            load_config.destination_dataset

    def test_destination_dataset_ifDatasetIsEmptyString_raisesValueError(self):
        load_config = EmsLoadJobConfig(destination_dataset="", schema=SCHEMA)

        with self.assertRaises(ValueError):
            load_config.destination_dataset

    def test_destination_table_ifTableIsNone_raisesValueError(self):
        load_config = EmsLoadJobConfig(destination_table=None, schema=SCHEMA)

        with self.assertRaises(ValueError):
            load_config.destination_table

    def test_destination_table_ifTableIsEmptyString_raisesValueError(self):
        load_config = EmsLoadJobConfig(destination_table="", schema=SCHEMA)

        with self.assertRaises(ValueError):
            load_config.destination_table
