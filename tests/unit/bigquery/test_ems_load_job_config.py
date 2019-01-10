from unittest import TestCase

from bigquery.ems_load_job_config import EmsLoadJobConfig


class TestEmsLoadJobConfig(TestCase):

    def test_destination_project_id_ifProjectIdIsNone_raisesValueError(self):
        load_config = EmsLoadJobConfig(destination_project_id=None)

        with self.assertRaises(ValueError):
            load_config.destination_project_id

    def test_destination_project_id_ifProjectIdIsEmptyString_raisesValueError(self):
        load_config = EmsLoadJobConfig(destination_project_id="")

        with self.assertRaises(ValueError):
            load_config.destination_project_id

    def test_destination_project_id_ifProjectIdIsMultipleWhitespaces_raisesValueError(self):
        load_config = EmsLoadJobConfig(destination_project_id="     \t  ")

        with self.assertRaises(ValueError):
            load_config.destination_project_id

    def test_destination_dataset_ifDatasetIsNone_raisesValueError(self):
        load_config = EmsLoadJobConfig(destination_dataset=None)

        with self.assertRaises(ValueError):
            load_config.destination_dataset

    def test_destination_dataset_ifDatasetIsEmptyString_raisesValueError(self):
        load_config = EmsLoadJobConfig(destination_dataset="")

        with self.assertRaises(ValueError):
            load_config.destination_dataset

    def test_destination_table_ifTableIsNone_raisesValueError(self):
        load_config = EmsLoadJobConfig(destination_table=None)

        with self.assertRaises(ValueError):
            load_config.destination_table

    def test_destination_table_ifTableIsEmptyString_raisesValueError(self):
        load_config = EmsLoadJobConfig(destination_table="")

        with self.assertRaises(ValueError):
            load_config.destination_table
