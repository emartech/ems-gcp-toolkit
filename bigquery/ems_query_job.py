from enum import Enum
from typing import Union


class EmsQueryState(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"


class EmsQueryJob:
    def __init__(self, job_id: str, query: str, state: EmsQueryState, error_result: Union[dict, None]):
        self.__job_id = job_id
        self.__state = state
        self.__error_result = error_result
        self.__query = query

    @property
    def state(self) -> EmsQueryState:
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

