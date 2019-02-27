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
