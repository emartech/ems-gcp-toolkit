from enum import Enum


class EmsWriteDisposition(Enum):
    WRITE_APPEND = "WRITE_APPEND"
    WRITE_TRUNCATE = "WRITE_TRUNCATE"
    WRITE_EMPTY = "WRITE_EMPTY"


class EmsCreateDisposition(Enum):
    CREATE_IF_NEEDED = "CREATE_IF_NEEDED"
    CREATE_NEVER = "CREATE_NEVER"


class EmsQueryPriority(Enum):
    INTERACTIVE = "INTERACTIVE"
    BATCH = "BATCH"


class EmsQueryConfig:

    def __init__(self,
                 priority: EmsQueryPriority = EmsQueryPriority.INTERACTIVE,
                 destination_dataset: str = None,
                 destination_table: str = None,
                 create_disposition: EmsCreateDisposition = EmsCreateDisposition.CREATE_IF_NEEDED,
                 write_disposition: EmsWriteDisposition = EmsWriteDisposition.WRITE_APPEND) -> None:
        self.__priority = priority
        self.__destination_dataset = destination_dataset
        self.__create_disposition = create_disposition
        self.__write_disposition = write_disposition
        self.__destination_table = destination_table

    @property
    def destination_dataset(self):
        return self.__destination_dataset

    @property
    def create_disposition(self):
        return self.__create_disposition

    @property
    def write_disposition(self):
        return self.__write_disposition

    @property
    def destination_table(self):
        return self.__destination_table

    @property
    def priority(self):
        return self.__priority
