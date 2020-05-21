from typing import Union

from bigquery.job.config.ems_load_job_config import EmsLoadJobConfig
from bigquery.job.ems_job import EmsJob
from bigquery.job.ems_job_state import EmsJobState


class EmsLoadJob(EmsJob):
    def __init__(self,
                 job_id: str,
                 load_config: EmsLoadJobConfig,
                 state: EmsJobState,
                 error_result: Union[dict, None]):
        super(EmsLoadJob, self).__init__(job_id, state, error_result)

        self.__load_config = load_config

    @property
    def load_config(self) -> EmsLoadJobConfig:
        return self.__load_config
