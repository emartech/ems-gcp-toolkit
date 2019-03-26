from typing import Union

from bigquery.job.config.ems_load_job_config import EmsLoadJobConfig
from bigquery.job.ems_job_state import EmsJobState


class EmsLoadJob:
    def __init__(self,
                 job_id: str,
                 load_config: EmsLoadJobConfig,
                 state: EmsJobState,
                 error_result: Union[dict, None]):
        self.__job_id = job_id
        self.__load_config = load_config
        self.__state = state
        self.__error_result = error_result

    @property
    def load_config(self) -> EmsLoadJobConfig:
        return self.__load_config

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
    def error_result(self):
        return self.__error_result
