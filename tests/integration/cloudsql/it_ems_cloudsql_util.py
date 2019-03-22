import datetime
from unittest import TestCase

from googleapiclient import discovery
from tenacity import retry, stop_after_delay, retry_if_result, wait_fixed

from cloudsql.ems_cloudsql_client import EmsCloudsqlClient
from cloudsql.ems_cloudsql_util import EmsCloudsqlUtil, TempBucketDescriptor
from storage.ems_storage_client import EmsStorageClient
from tests.integration import GCP_PROJECT_ID
from tests.integration import GCP_CLOUDSQL_INSTANCE_ID

DATABASE = "ems-gcp-toolkit-test"
DISCOVERY_SERVICE = discovery.build("sqladmin", "v1beta4", cache_discovery=False)
BUCKET_NAME = GCP_PROJECT_ID + "-gcp-toolkit-it"
IMPORT_USER = "postgres"
JOB_TIMEOUT_SECONDS = 30


# TODO  extract common test utils for
# (__get_table_content, get test bucket, __create_input_csv, __drop_and_create_table), retry

class ItEmsCloudsqlUtil(TestCase):

    def setUp(self):
        self.__storage_client = EmsStorageClient(GCP_PROJECT_ID)
        temp_bucket = TempBucketDescriptor(
            GCP_PROJECT_ID,
            GCP_CLOUDSQL_INSTANCE_ID + "-temp-bucket",
            "europe-west1"
        )
        self.__cloud_sql_client = EmsCloudsqlClient(GCP_PROJECT_ID,
                                                    GCP_CLOUDSQL_INSTANCE_ID)

        self.__util = EmsCloudsqlUtil(self.__cloud_sql_client, self.__storage_client, temp_bucket)
        self.__storage_client.create_bucket_if_not_exists(BUCKET_NAME, GCP_PROJECT_ID, "europe-west1")

    def test_run_sql(self):
        table_name = "test_table"
        suffix = str(int(datetime.datetime.utcnow().timestamp()))
        blob_name = f"export_{suffix}.csv"
        query = f'''DROP TABLE IF EXISTS {table_name};
                              CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, name VARCHAR);
                              INSERT INTO {table_name} VALUES (3, 'old foo'), (4, 'old bar');'''

        self.__util.run_sql(DATABASE, query, IMPORT_USER, JOB_TIMEOUT_SECONDS)

        self.__export_table_to_csv(f"gs://{BUCKET_NAME}/{blob_name}", table_name)
        lines = self.__storage_client.download_lines(BUCKET_NAME, blob_name, num_lines=3)
        self.assertIn("3,old foo", lines)
        self.assertIn("4,old bar", lines)

    def __get_table_content(self, table_name):
        suffix = str(int(datetime.datetime.utcnow().timestamp()))
        blob_name = f"export_{suffix}.csv"
        self.__export_table_to_csv(f"gs://{BUCKET_NAME}/{blob_name}", table_name)
        content = self.__storage_client.download_content_as_string(BUCKET_NAME, blob_name)
        self.__storage_client.delete_blob(BUCKET_NAME, blob_name)
        return content

    def __create_input_csv(self, content):
        suffix = str(int(datetime.datetime.utcnow().timestamp()))
        blob_name = f"input_{suffix}.csv"
        self.__storage_client.upload_from_string(BUCKET_NAME, blob_name, content)

        return f"gs://{BUCKET_NAME}/{blob_name}"

    def __create_table_with_dumy_values(self, table_name):
        create_table_sql = f'''DROP TABLE IF EXISTS {table_name};
                                      CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, name VARCHAR);
                                       INSERT INTO {table_name} VALUES (3, 'old foo'), (4, 'old bar');'''

        blob_name = f"create_test_table.sql"
        self.__storage_client.upload_from_string(BUCKET_NAME, blob_name, create_table_sql)

        blob_name = f"create_test_table.sql"
        request_body = {
            "importContext": {
                "kind": "sql#importContext",
                "fileType": "SQL",
                "uri": "gs://" + BUCKET_NAME + "/" + blob_name,
                "database": DATABASE,
                "importUser": IMPORT_USER
            }
        }
        request = DISCOVERY_SERVICE.instances().import_(project=GCP_PROJECT_ID,
                                                        instance=GCP_CLOUDSQL_INSTANCE_ID,
                                                        body=request_body)
        self.__wait_for_job_done(request.execute()["name"])

        self.__storage_client.delete_blob(BUCKET_NAME, blob_name)

    def __export_table_to_csv(self, export_uri, table_name):
        export_request_body = {
            "exportContext": {
                "kind": "sql#exportContext",
                "fileType": "CSV",
                "uri": export_uri,
                "databases": [
                    DATABASE
                ],
                "csvExportOptions": {
                    "selectQuery": f"select * from {table_name} "
                }
            }
        }

        request = DISCOVERY_SERVICE.instances().export(project=GCP_PROJECT_ID,
                                                       instance=GCP_CLOUDSQL_INSTANCE_ID,
                                                       body=export_request_body)
        response = request.execute()

        status = self.__wait_for_job_done(response["name"])
        assert "error" not in status, f"Status: {status}"

    @staticmethod
    @retry(stop=(stop_after_delay(JOB_TIMEOUT_SECONDS)),
           retry=(retry_if_result(lambda result: result["status"] != "DONE")),
           wait=wait_fixed(2))
    def __wait_for_job_done(ops_id):
        ops_request = DISCOVERY_SERVICE.operations().get(project=GCP_PROJECT_ID, operation=ops_id)
        ops_response = ops_request.execute()
        return ops_response
