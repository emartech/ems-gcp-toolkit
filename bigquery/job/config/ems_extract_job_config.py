from enum import Enum

from bigquery.job.config.ems_job_config import EmsJobConfig


class DestinationFormat(Enum):
    CSV = "CSV"
    NEWLINE_DELIMITED_JSON = "NEWLINE_DELIMITED_JSON"
    AVRO = "AVRO"


class Compression(Enum):
    GZIP = "GZIP"
    DEFLATE = "DEFLATE"
    SNAPPY = "SNAPPY"
    NONE = "NONE"


class EmsExtractJobConfig(EmsJobConfig):

    def __init__(self,
                 compression: Compression = Compression.NONE,
                 destination_format: DestinationFormat = DestinationFormat.CSV,
                 field_delimiter: str = None,
                 print_header: bool = False,
                 *args, **kwargs):
        super(EmsExtractJobConfig, self).__init__(*args, **kwargs)
        self.__print_header = print_header
        self.__destination_format = destination_format
        self.__field_delimiter = field_delimiter
        self.__compression = compression

    @property
    def print_header(self):
        return self.__print_header

    @property
    def destination_format(self):
        return self.__destination_format

    @property
    def field_delimiter(self):
        return self.__field_delimiter

    @property
    def compression(self):
        return self.__compression
