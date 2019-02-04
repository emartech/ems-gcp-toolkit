import datetime

from google.api_core.exceptions import NotFound
from google.cloud import storage
from googleapiclient import discovery
from tenacity import retry, stop_after_delay, retry_if_result, wait_fixed


class EmsCloudsqlClient:
    IMPORT_CSV_TIMEOUT = 200
    CREATE_TMP_TABLE_TIMEOUT = 30
    RELOAD_TABLE_TIMEOUT = 200

    def __init__(self, project_id, instance_id):
        self.__project_id = project_id
        self.__bucket_name = project_id + "-tmp-bucket"
        self.__instance_id = instance_id
        self.__discovery_service = discovery.build('sqladmin', 'v1beta4')
        self.__storage_client = storage.Client(project_id)

    def load_table_from_blob(self, database, table_name, source_uri):
        tmp_table_name = self.__create_tmp_table_from(database, table_name)
        self.__import_csv_from_bucket(database, tmp_table_name, source_uri, self.IMPORT_CSV_TIMEOUT)
        self.__reload_table_from_tmp(database, tmp_table_name, table_name)

    def run_sql(self, database, sql_query, timeout_seconds):
        suffix = str(int(datetime.datetime.utcnow().timestamp()))
        blob_name = f"sql_query_{suffix}"
        self.__save_to_bucket(sql_query, blob_name)
        self.__import_sql_from_bucket(database, f"gs://{self.__bucket_name}/{blob_name}", timeout_seconds)
        bucket = self.__get_or_create_bucket(self.__bucket_name)
        bucket.delete_blob(blob_name)

    def __create_tmp_table_from(self, database, source_table):
        tmp_table = f"tmp_{source_table}"
        sql_query = f"""DROP TABLE IF EXISTS  {tmp_table} ;
            CREATE TABLE {tmp_table} AS SELECT * FROM {source_table} WHERE False;"""
        self.run_sql(database, sql_query, self.CREATE_TMP_TABLE_TIMEOUT)
        return tmp_table

    def __reload_table_from_tmp(self, database, source_table, destination_table):
        sql_query = f"""TRUNCATE TABLE {destination_table};
            insert into {destination_table} select * from {source_table};
            drop table {source_table};"""
        self.run_sql(database, sql_query, self.RELOAD_TABLE_TIMEOUT)

    def __import_csv_from_bucket(self, database, destination_table_name, source_csv_uri, timeout_seconds):
        import_request_body = {
            "importContext": {
                "kind": "sql#importContext",
                "fileType": "CSV",
                "uri": source_csv_uri,
                "database": database,
                "csvImportOptions": {
                    "table": destination_table_name
                }
            }
        }
        request = self.__discovery_service.instances().import_(project=self.__project_id,
                                                               instance=self.__instance_id,
                                                               body=import_request_body)
        self.__wait_for_job_done(request.execute()["name"], timeout_seconds)

    def __import_sql_from_bucket(self, database, source_sql_uri, timeout_seconds):
        import_user = "postgres"
        request_body = {
            "importContext": {
                "kind": "sql#importContext",
                "fileType": "SQL",
                "uri": source_sql_uri,
                "database": database,
                "importUser": import_user
            }
        }
        request = self.__discovery_service.instances().import_(project=self.__project_id,
                                                               instance=self.__instance_id,
                                                               body=request_body)
        self.__wait_for_job_done(request.execute()["name"], timeout_seconds)

    def __get_or_create_bucket(self, bucket_name):
        try:
            bucket = self.__storage_client.get_bucket(bucket_name)
        except NotFound:
            bucket = self.__storage_client.bucket(bucket_name)
            bucket.location = "europe-west1"
            bucket.storage_class = "REGIONAL"
            bucket.create()
        return bucket

    def __save_to_bucket(self, content, name):
        bucket = self.__get_or_create_bucket(self.__bucket_name)
        blob = bucket.blob(name)
        blob.upload_from_string(content)

    def __wait_for_job_done(self, ops_id, timeout_seconds):
        @retry(wait=wait_fixed(1),
               stop=(stop_after_delay(timeout_seconds)),
               retry=(retry_if_result(lambda result: result["status"] != "DONE")))
        def __wait_for_job_done_helper():
            ops_request = self.__discovery_service.operations().get(project=self.__project_id, operation=ops_id)
            ops_response = ops_request.execute()
            return ops_response

        status = __wait_for_job_done_helper()
        if "error" in status:
            raise EmsCloudsqlClientError(f"job failed with error status {status}")


class EmsCloudsqlClientError(Exception):
    def __init__(self, message):
        super(EmsCloudsqlClientError, self).__init__(message)
