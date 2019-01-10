from enum import Enum
from typing import Union

from bigquery.ems_job_config import EmsJobConfig


class EmsLoadState(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"


class EmsLoadJob:
    def __init__(self,
                 job_id: str,
                 load_config: EmsJobConfig,
                 state: EmsLoadState,
                 error_result: Union[dict, None]):
        self.__job_id = job_id
        self.__load_config = load_config
        self.__state = state
        self.__error_result = error_result

    @property
    def load_config(self) -> EmsJobConfig:
        return self.__load_config

    @property
    def state(self) -> EmsLoadState:
        return self.__state

    @property
    def job_id(self) -> str:
        return self.__job_id

    @property
    def is_failed(self) -> bool:
        return self.__error_result is not None
