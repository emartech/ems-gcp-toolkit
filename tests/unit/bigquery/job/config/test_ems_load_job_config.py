from unittest import TestCase

from bigquery.job.config.ems_job_config import EmsCreateDisposition, EmsWriteDisposition
from bigquery.job.config.ems_load_job_config import EmsLoadJobConfig

SCHEMA = {"fields": [{"type": "INT64", "name": "f"}]}


class TestEmsLoadJobConfig(TestCase):

    def setUp(self):
        self.ems_load_job_config = EmsLoadJobConfig(destination_project_id="test_project",
                                                    destination_dataset="test_dataset",
                                                    destination_table="test_table",
                                                    create_disposition=EmsCreateDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=EmsWriteDisposition.WRITE_APPEND,
                                                    schema=SCHEMA,
                                                    source_uri_template="gs://bucket_id/{blob_id}")

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

    def test_schema(self):
        self.assertEqual(self.ems_load_job_config.schema, SCHEMA)

    def test_source_uri_template(self):
        self.assertEqual(self.ems_load_job_config.source_uri_template, "gs://bucket_id/{blob_id}")

    def test_destination_project_id_ifProjectIdIsNone_raisesValueError(self):
        with self.assertRaises(ValueError):
            EmsLoadJobConfig(destination_project_id=None, schema=SCHEMA, source_uri_template="")

    def test_destination_project_id_ifProjectIdIsEmptyString_raisesValueError(self):
        with self.assertRaises(ValueError):
            EmsLoadJobConfig(destination_project_id="", schema=SCHEMA, source_uri_template="")

    def test_destination_project_id_ifProjectIdIsMultipleWhitespaces_raisesValueError(self):
        with self.assertRaises(ValueError):
            EmsLoadJobConfig(destination_project_id="     \t  ", schema=SCHEMA, source_uri_template="")

    def test_destination_dataset_ifDatasetIsNone_raisesValueError(self):
        with self.assertRaises(ValueError):
            EmsLoadJobConfig(destination_dataset=None, schema=SCHEMA, source_uri_template="")

    def test_destination_dataset_ifDatasetIsEmptyString_raisesValueError(self):
        with self.assertRaises(ValueError):
            EmsLoadJobConfig(destination_dataset="", schema=SCHEMA, source_uri_template="")

    def test_destination_table_ifTableIsNone_raisesValueError(self):
        with self.assertRaises(ValueError):
            EmsLoadJobConfig(destination_table=None, schema=SCHEMA, source_uri_template="")

    def test_destination_table_ifTableIsEmptyString_raisesValueError(self):
        with self.assertRaises(ValueError):
            EmsLoadJobConfig(destination_table="", schema=SCHEMA, source_uri_template="")
