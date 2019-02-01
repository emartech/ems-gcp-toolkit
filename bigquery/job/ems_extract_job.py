from typing import List, Union

from bigquery.job.ems_job_state import EmsJobState


class EmsExtractJob:
    def __init__(self,
                 job_id: str,
                 table: str,
                 destination_uris: List[str],
                 state: EmsJobState,
                 error_result: Union[dict, None]):
        self.__job_id = job_id
        self.__table = table
        self.__destination_uris = destination_uris
        self.__state = state
        self.__error_result = error_result

    @property
    def job_id(self) -> str:
        return self.__job_id

    @property
    def is_failed(self) -> bool:
        return self.__error_result is not None

    @property
    def state(self) -> EmsJobState:
        return self.__state

    @property
    def table(self) -> str:
        return self.__table

    @property
    def destination_uris(self) -> List[str]:
        return self.__destination_uris

    @property
    def error_result(self) -> Union[dict, None]:
        return self.__error_result
