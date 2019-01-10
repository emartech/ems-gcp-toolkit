from bigquery.ems_job_config import EmsJobConfig


class EmsQueryJobConfig(EmsJobConfig):

    def __init__(self, *args, **kwargs):
        super(EmsQueryJobConfig, self).__init__(*args, **kwargs)