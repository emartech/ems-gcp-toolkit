from typing import Union

from bigquery.job.config.ems_query_job_config import EmsQueryJobConfig
from bigquery.job.ems_job_state import EmsJobState


class EmsQueryJob:
    def __init__(self,
                 job_id: str,
                 query: str,
                 query_config: EmsQueryJobConfig,
                 state: EmsJobState,
                 error_result: Union[dict, None]):
        self.__job_id = job_id
        self.__query = query
        self.__query_config = query_config
        self.__state = state
        self.__error_result = error_result

    @property
    def query_config(self) -> EmsQueryJobConfig:
        return self.__query_config

    @property
    def state(self) -> EmsJobState:
        return self.__state

    @property
    def job_id(self) -> str:
        return self.__job_id

    @property
    def is_failed(self) -> bool:
        return self.__error_result is not None

    @property
    def query(self) -> str:
        return self.__query

    @property
    def error_result(self):
        return self.__error_result

