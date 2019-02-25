import datetime
import logging
from typing import NamedTuple

from cloudsql.ems_cloudsql_client import EmsCloudsqlClient
from storage.ems_storage_client import EmsStorageClient

LOGGER = logging.getLogger(__name__)


# pylint: disable=C0103
class TempBucketDescriptor(NamedTuple):
    project_id: str
    name: str
    location: str


class EmsCloudsqlUtil:
    IMPORT_CSV_TIMEOUT = 600
    RELOAD_TABLE_TIMEOUT = 600
    CREATE_TMP_TABLE_TIMEOUT = 30

    def __init__(self,
                 cloud_sql_client: EmsCloudsqlClient,
                 storage_client: EmsStorageClient,
                 temp_bucket: TempBucketDescriptor):
        self.__temp_bucket = temp_bucket
        self.__storage_client = storage_client
        self.__cloud_sql_client = cloud_sql_client

    def run_sql(self, database: str, sql_query: str, import_user: str, timeout_seconds: float = 30) -> None:
        suffix = str(int(datetime.datetime.utcnow().timestamp()))
        blob_name = f"sql_query_{suffix}"
        LOGGER.info("Running SQL %s in database %s", sql_query, database)
        self.__storage_client.create_bucket_if_not_exists(bucket_name=self.__temp_bucket.name,
                                                          project=self.__temp_bucket.project_id,
                                                          location=self.__temp_bucket.location)
        self.__storage_client.upload_from_string(bucket_name=self.__temp_bucket.name,
                                                 blob_name=blob_name,
                                                 content=sql_query)
        self.__cloud_sql_client.import_sql_from_bucket(database=database,
                                                       source_sql_uri=f"gs://{self.__temp_bucket.name}/{blob_name}",
                                                       timeout_seconds=timeout_seconds,
                                                       import_user=import_user)
        self.__storage_client.delete_blob(bucket_name=self.__temp_bucket.name,
                                          blob_name=blob_name)

    def reload_table_from_blob(self, database: str, table_name: str, source_uri: str, import_user: str) -> None:
        tmp_table_name = self.__create_tmp_table_from(database, table_name, import_user)
        self.__cloud_sql_client.import_csv_from_bucket(database, tmp_table_name, source_uri, self.IMPORT_CSV_TIMEOUT)
        self.__reload_table_from_tmp(database, tmp_table_name, table_name, import_user)

    def __create_tmp_table_from(self, database: str, source_table: str, import_user: str) -> str:
        tmp_table = f"tmp_{source_table}"
        LOGGER.info("Creating tmp table %s in database %s with schema from table %s", tmp_table, database, source_table)
        sql_query = f'''DROP TABLE IF EXISTS  {tmp_table} ;
               CREATE TABLE {tmp_table} AS SELECT * FROM {source_table} WHERE False;'''
        self.run_sql(database, sql_query, import_user, self.CREATE_TMP_TABLE_TIMEOUT)
        return tmp_table

    def __reload_table_from_tmp(self, database: str, source_table: str, destination_table: str, import_user) -> None:
        sql_query = f'''TRUNCATE TABLE {destination_table};
               insert into {destination_table} select * from {source_table};
               drop table {source_table};'''
        self.run_sql(database, sql_query, import_user, self.RELOAD_TABLE_TIMEOUT)
