from bigquery.job.config.ems_job_config import EmsJobConfig


class EmsLoadJobConfig(EmsJobConfig):
    
    def __init__(self, schema: dict, source_uri_template: str, *args, **kwargs):
        super(EmsLoadJobConfig, self).__init__(*args, **kwargs)
        self.__schema_json = schema
        self.__source_uri_template = source_uri_template

    @property
    def source_uri_template(self):
        return self.__source_uri_template

    @property
    def schema(self):
        return self.__schema_json

    @property
    def destination_project_id(self):
        destination_project_id = super().destination_project_id
        self.__validate(destination_project_id)

        return destination_project_id

    @property
    def destination_dataset(self):
        destination_dataset = super().destination_dataset
        self.__validate(destination_dataset)

        return destination_dataset

    @property
    def destination_table(self):
        destination_table = super().destination_table
        self.__validate(destination_table)

        return destination_table

    def __validate(self, value):
        if value is None or value.strip() == "":
            raise ValueError("Cannot be None or empty string!")