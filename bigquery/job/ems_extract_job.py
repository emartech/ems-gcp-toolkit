from typing import List, Union

from bigquery.job.ems_job import EmsJob
from bigquery.job.ems_job_state import EmsJobState


class EmsExtractJob(EmsJob):
    def __init__(self,
                 job_id: str,
                 table: str,
                 destination_uris: List[str],
                 state: EmsJobState,
                 error_result: Union[dict, None]):
        super(EmsExtractJob, self).__init__(job_id, state, error_result)

        self.__table = table
        self.__destination_uris = destination_uris

    @property
    def table(self) -> str:
        return self.__table

    @property
    def destination_uris(self) -> List[str]:
        return self.__destination_uris
