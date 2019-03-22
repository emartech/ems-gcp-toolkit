import datetime
from unittest import TestCase

from google.cloud import storage
from google.cloud.exceptions import NotFound
from googleapiclient import discovery
from tenacity import retry, stop_after_delay, retry_if_result, wait_fixed

from cloudsql.ems_cloudsql_client import EmsCloudsqlClient
from tests.integration import GCP_PROJECT_ID
from tests.integration import GCP_CLOUDSQL_INSTANCE_ID


class ItEmsCloudsqlClient(TestCase):
    DATABASE = "ems-gcp-toolkit-test"
    DISCOVERY_SERVICE = discovery.build("sqladmin", "v1beta4", cache_discovery=False)
    BUCKET_NAME = GCP_PROJECT_ID + "-gcp-toolkit-it"
    IMPORT_USER = "postgres"
    JOB_TIMEOUT_SECONDS = 30

    def setUp(self):
        self.__storage_client = storage.Client(GCP_PROJECT_ID)
        self.__client = EmsCloudsqlClient(GCP_PROJECT_ID,
                                          GCP_CLOUDSQL_INSTANCE_ID)

    def test_import_csv_from_bucket_importsData(self):
        table_name = "testtable"
        self.__drop_and_create_table(table_name)

        csv_uri = self.__create_input_csv(f"1,alma\n")

        self.__client.import_csv_from_bucket(self.DATABASE, table_name, csv_uri, self.JOB_TIMEOUT_SECONDS)

        loaded_data = self.__get_table_content(table_name)
        self.assertEqual(loaded_data, f"1,alma\n")

    def test_import_sql_from_bucket_importsSql(self):
        table_name = "existing"
        content_to_load = f'''DROP TABLE IF EXISTS {table_name};
                              CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, name VARCHAR);
                              INSERT INTO {table_name} VALUES (1, 'foo');'''
        bucket = self.__get_test_bucket(self.BUCKET_NAME)
        suffix = str(int(datetime.datetime.utcnow().timestamp()))
        blob_name = f"input_{suffix}.csv"
        blob = bucket.blob(blob_name)
        blob.upload_from_string(content_to_load)
        source_uri = f"gs://{self.BUCKET_NAME}/{blob_name}"

        self.__client.import_sql_from_bucket(self.DATABASE, source_uri, 30, self.IMPORT_USER)

        loaded_data = self.__get_table_content(table_name)
        self.assertEqual(loaded_data, "1,foo\n")

    def __get_test_bucket(self, bucket_name=BUCKET_NAME):

        try:
            bucket = self.__storage_client.get_bucket(bucket_name)
        except NotFound:
            bucket = self.__storage_client.bucket(bucket_name)
            bucket.location = "europe-west1"
            bucket.storage_class = "REGIONAL"
            bucket.create()
        return bucket

    # error cases:
    # blob does not exist
    # bucket does not exist
    # import was unsuccessful
    # delete temp blobs

    def __drop_and_create_table(self, table_name):
        create_table_sql = f'''DROP TABLE IF EXISTS {table_name};
                              CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, name VARCHAR);'''

        blob_name = f"create_test_table.sql"
        self.__get_test_bucket().blob(blob_name).upload_from_string(create_table_sql)
        request_body = {
            "importContext": {
                "kind": "sql#importContext",
                "fileType": "SQL",
                "uri": "gs://" + self.BUCKET_NAME + "/" + blob_name,
                "database": self.DATABASE,
                "importUser": self.IMPORT_USER
            }
        }
        request = self.DISCOVERY_SERVICE.instances().import_(project=GCP_PROJECT_ID,
                                                             instance=GCP_CLOUDSQL_INSTANCE_ID,
                                                             body=request_body)
        self.__wait_for_job_done(request.execute()["name"])

        self.__get_test_bucket().blob(blob_name).delete()

    def __create_input_csv(self, content):
        bucket = self.__get_test_bucket(self.BUCKET_NAME)
        suffix = str(int(datetime.datetime.utcnow().timestamp()))
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
                                                            instance=GCP_CLOUDSQL_INSTANCE_ID,
                                                            body=export_request_body)
        response = request.execute()

        status = self.__wait_for_job_done(response["name"])
        assert "error" not in status, f"Status: {status}"

    @retry(stop=(stop_after_delay(JOB_TIMEOUT_SECONDS)),
           retry=(retry_if_result(lambda result: result["status"] != "DONE")),
           wait=wait_fixed(2))
    def __wait_for_job_done(self, ops_id):
        ops_request = self.DISCOVERY_SERVICE.operations().get(project=GCP_PROJECT_ID, operation=ops_id)
        ops_response = ops_request.execute()
        return ops_response

# delete temp table

# wait for bucket upload ????
