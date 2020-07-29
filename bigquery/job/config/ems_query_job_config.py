from enum import Enum

from bigquery.job.config.ems_job_config import EmsJobConfig, EmsJobPriority


class EmsTimePartitioningType(Enum):
    DAY = "DAY"


class EmsTimePartitioning:
    def __init__(self,
                 type_: EmsTimePartitioningType = None,
                 field: str = None,
                 expiration_ms: int = None,
                 require_partition_filter: bool = None):
        if type_ is None:
            self.__type = EmsTimePartitioningType.DAY
        else:
            self.__type = type_
        self.__field = field
        self.__expiration_ms = expiration_ms
        self.__require_partition_filter = require_partition_filter

    @property
    def type(self):
        return self.__type

    @property
    def field(self):
        return self.__field

    @property
    def expiration_ms(self):
        return self.__expiration_ms

    @property
    def require_partition_filter(self):
        return self.__require_partition_filter


class EmsQueryJobConfig(EmsJobConfig):

    def __init__(self,
                 priority: EmsJobPriority = EmsJobPriority.INTERACTIVE,
                 use_query_cache: bool = False,
                 time_partitioning: EmsTimePartitioning = None,
                 *args, **kwargs):
        super(EmsQueryJobConfig, self).__init__(*args, **kwargs)
        self.__priority = priority
        self.__use_query_cache = use_query_cache
        self.__time_partitioning = time_partitioning

    @property
    def priority(self):
        return self.__priority

    @property
    def use_query_cache(self):
        return self.__use_query_cache

    @property
    def time_partitioning(self):
        return self.__time_partitioning
