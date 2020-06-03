from datetime import datetime
from typing import List, Union

from bigquery.job.config.ems_extract_job_config import EmsExtractJobConfig
from bigquery.job.ems_job import EmsJob
from bigquery.job.ems_job_state import EmsJobState


class EmsExtractJob(EmsJob):
    def __init__(self,
                 job_id: str,
                 table: str,
                 destination_uris: List[str],
                 job_config: EmsExtractJobConfig,
                 state: EmsJobState,
                 error_result: Union[dict, None],
                 created: datetime = None):
        super(EmsExtractJob, self).__init__(job_id, state, error_result, created)

        self.__job_config = job_config
        self.__table = table
        self.__destination_uris = destination_uris

    @property
    def table(self) -> str:
        return self.__table

    @property
    def job_config(self) -> EmsExtractJobConfig:
        return self.__job_config

    @property
    def destination_uris(self) -> List[str]:
        return self.__destination_uris
