import datetime
from unittest import TestCase

from gcloud.exceptions import NotFound
from google.cloud import storage
from googleapiclient import discovery
from tenacity import retry, stop_after_delay, retry_if_result

from cloudsql.ems_cloudsql_client import EmsCloudsqlClient, EmsCloudsqlClientError
from tests.integration import GCP_PROJECT_ID


# TODO these are client tests, should be moved, here should be tests for importing, not creating tables, blobs, etc
class ItEmsCloudSqlClient(TestCase):
    GCP_CLOUDSQL_INSTANCE_ID = "ems-replenishment-dev"
    DATABASE = "ems-gcp-toolkit-test"
    DISCOVERY_SERVICE = discovery.build("sqladmin", "v1beta4", cache_discovery=False)
    BUCKET_NAME = GCP_PROJECT_ID + "-gcp-toolkit-it"
    IMPORT_USER = "postgres"
    JOB_TIMEOUT_SECONDS = 30

    def setUp(self):
        self.__storage_client = storage.Client(GCP_PROJECT_ID)
        self.__client = EmsCloudsqlClient("ems-data-platform-dev",
                                          self.GCP_CLOUDSQL_INSTANCE_ID,
                                          self.GCP_CLOUDSQL_INSTANCE_ID + "-temp-bucket",
                                          "europe-west1")

    def __get_test_bucket(self, bucket_name):

        try:
            bucket = self.__storage_client.get_bucket(bucket_name)
        except NotFound:
            bucket = self.__storage_client.bucket(bucket_name)
            bucket.location = "europe-west1"
            bucket.storage_class = "REGIONAL"
            bucket.create()
        return bucket

    def test_load_table_from_blob_throwsExceptionIfTableDoesNotExist(self):
        with self.assertRaises(EmsCloudsqlClientError):
            table_name = "notexists"
            source_uri = self.__create_input_csv("2, foo\n")
            self.__client.reload_table_from_blob(self.DATABASE, table_name, source_uri, self.IMPORT_USER)

    def test_load_table_from_blob_overwritesTable(self):
        table_name = "existing"
        self.__create_table_with_dumy_values(table_name)

        content_to_load = "2, foo\n"
        source_uri = self.__create_input_csv(content_to_load)

        self.__client.reload_table_from_blob(self.DATABASE, table_name, source_uri, self.IMPORT_USER)

        loaded_data = self.__get_table_content(table_name)
        self.assertEqual(loaded_data, content_to_load)

    def __create_table_with_dumy_values(self, table_name):
        self.__client.run_sql(self.DATABASE,
                              f'''DROP TABLE IF EXISTS {table_name};
                              CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, name VARCHAR);
                              INSERT INTO {table_name} VALUES (3, 'old foo'), (4, 'old bar');''',
                              self.JOB_TIMEOUT_SECONDS,
                              self.IMPORT_USER)

    def __create_input_csv(self, content):
        suffix = str(int(datetime.datetime.utcnow().timestamp()))
        bucket = self.__get_test_bucket(self.BUCKET_NAME)
        blob_name = f"input_{suffix}.csv"
        blob = bucket.blob(blob_name)
        blob.upload_from_string(content)
        return f"gs://{self.BUCKET_NAME}/{blob_name}"

    def __get_table_content(self, table_name):
        suffix = str(int(datetime.datetime.utcnow().timestamp()))
        bucket = self.__get_test_bucket(self.BUCKET_NAME)
        blob_name = f"export_{suffix}.csv"
        self.__export_table_to_csv(f"gs://{self.BUCKET_NAME}/{blob_name}", table_name)
        content = bucket.blob(blob_name).download_as_string().decode("utf-8")
        bucket.delete_blob(blob_name)
        return content

    def __export_table_to_csv(self, export_uri, table_name):
        export_request_body = {
            "exportContext": {
                "kind": "sql#exportContext",
                "fileType": "CSV",
                "uri": export_uri,
                "databases": [
                    self.DATABASE
                ],
                "csvExportOptions": {
                    "selectQuery": f"select * from {table_name} "
                }
            }
        }

        request = self.DISCOVERY_SERVICE.instances().export(project=GCP_PROJECT_ID,
                                                            instance=self.GCP_CLOUDSQL_INSTANCE_ID,
                                                            body=export_request_body)
        response = request.execute()

        status = self.__wait_for_job_done(response["name"])
        assert "error" not in status, f"Status: {status}"

    @retry(stop=(stop_after_delay(JOB_TIMEOUT_SECONDS)),
           retry=(retry_if_result(lambda result: result["status"] != "DONE")))
    def __wait_for_job_done(self, ops_id):
        ops_request = self.DISCOVERY_SERVICE.operations().get(project=GCP_PROJECT_ID, operation=ops_id)
        ops_response = ops_request.execute()
        return ops_response

# delete temp table

# wait for bucket upload ????
