from bigquery.job.config.ems_job_config import EmsJobConfig, EmsJobPriority


class EmsQueryJobConfig(EmsJobConfig):

    def __init__(self,
                 priority: EmsJobPriority = EmsJobPriority.INTERACTIVE,
                 use_query_cache: bool = False,
                 *args, **kwargs):
        super(EmsQueryJobConfig, self).__init__(*args, **kwargs)
        self.__priority = priority
        self.__use_query_cache = use_query_cache

    @property
    def priority(self):
        return self.__priority

    @property
    def use_query_cache(self):
        return self.__use_query_cache
