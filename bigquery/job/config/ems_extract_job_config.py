from bigquery.job.config.ems_job_config import EmsJobConfig


class EmsExtractJobConfig(EmsJobConfig):

    def __init__(self,
                 compression,
                 destination_format,
                 field_delimiter,
                 print_header,
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
