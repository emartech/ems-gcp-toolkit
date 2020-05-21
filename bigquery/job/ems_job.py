from abc import ABC
from typing import Union

from bigquery.job.ems_job_state import EmsJobState


class EmsJob(ABC):

    def __init__(self,
                 job_id: str,
                 state: EmsJobState,
                 error_result: Union[dict, None]):
        self.__job_id = job_id
        self.__state = state
        self.__error_result = error_result

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
    def error_result(self) -> Union[dict, None]:
        return self.__error_result
