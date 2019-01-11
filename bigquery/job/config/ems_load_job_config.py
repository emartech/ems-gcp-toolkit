from bigquery.job.config.ems_job_config import EmsJobConfig


class EmsLoadJobConfig(EmsJobConfig):
    
    def __init__(self, *args, **kwargs):
        super(EmsLoadJobConfig, self).__init__(*args, **kwargs)

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