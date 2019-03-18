import logging

from googleapiclient import discovery
from googleapiclient.errors import HttpError
from tenacity import retry, stop_after_delay, retry_if_result, wait_fixed, retry_if_exception_type

LOGGER = logging.getLogger(__name__)


class EmsCloudsqlClient:
    IMPORT_CSV_TIMEOUT = 600
    RELOAD_TABLE_TIMEOUT = 600
    CREATE_TMP_TABLE_TIMEOUT = 30

    def __init__(self, project_id: str, instance_id: str):
        self.__project_id = project_id

        self.__instance_id = instance_id
        self.__discovery_service = discovery.build("sqladmin", "v1beta4", cache_discovery=False)

    @property
    def project_id(self) -> str:
        return self.__project_id

    @property
    def instance_id(self) -> str:
        return self.__instance_id

    def import_csv_from_bucket(self, database: str, destination_table_name: str, source_csv_uri: str,
                               timeout_seconds: float) -> None:
        LOGGER.info("Importing CSV from %s to table %s in database %s",
                    source_csv_uri, destination_table_name, database)
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

    def import_sql_from_bucket(self, database: str, source_sql_uri: str, timeout_seconds: float,
                               import_user: str) -> None:
        LOGGER.info("Importing SQL from %s to database %s", source_sql_uri, database)
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

    # TODO should be implemented a public decorator for CloudSql discovery api calls
    def __wait_for_job_done(self, ops_id: str, timeout_seconds: float) -> None:
        LOGGER.info("Waiting for job %s to be done", ops_id)

        @retry(wait=wait_fixed(1),
               stop=stop_after_delay(timeout_seconds),
               retry=retry_if_result(lambda result: result["status"] != "DONE") | retry_if_exception_type(HttpError))
        def __wait_for_job_done_helper() -> dict:
            ops_request = self.__discovery_service.operations().get(project=self.__project_id, operation=ops_id)
            ops_response = ops_request.execute()
            return ops_response

        status = __wait_for_job_done_helper()
        if "error" in status:
            raise EmsCloudsqlClientError(f"job failed with error status {status}")


class EmsCloudsqlClientError(Exception):
    pass
