from bigquery.job.config.ems_job_config import EmsJobConfig, EmsJobPriority


class EmsQueryJobConfig(EmsJobConfig):

    def __init__(self,
                 priority: EmsJobPriority = EmsJobPriority.INTERACTIVE,
                 *args, **kwargs):
        super(EmsQueryJobConfig, self).__init__(*args, **kwargs)
        self.__priority = priority

    @property
    def priority(self):
        return self.__priority
