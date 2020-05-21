from typing import Union

from bigquery.job.config.ems_query_job_config import EmsQueryJobConfig
from bigquery.job.ems_job import EmsJob
from bigquery.job.ems_job_state import EmsJobState


class EmsQueryJob(EmsJob):
    def __init__(self,
                 job_id: str,
                 query: str,
                 query_config: EmsQueryJobConfig,
                 state: EmsJobState,
                 error_result: Union[dict, None]):
        super(EmsQueryJob, self).__init__(job_id, state, error_result)

        self.__query = query
        self.__query_config = query_config

    @property
    def query_config(self) -> EmsQueryJobConfig:
        return self.__query_config

    @property
    def query(self) -> str:
        return self.__query
