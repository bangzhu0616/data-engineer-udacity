from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataKeyCheckOperator(BaseOperator):

    ui_color = '#89DA59'

    key_names = {'pestcide_use': 'pestcide_use_id',
                 'compounds': 'compound_id',
                 'counties': 'county_id',
                 'states': 'state_code'}

    @apply_defaults
    def __init__(self,
                 aws_credentials_id='',
                 redshift_conn_id='',
                 tables=[],
                 *args, **kwargs):

        super(DataKeyCheckOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        for t in self.tables:
            records = redshift.get_records(f"SELECT {self.key_names[t]}, count(*) FROM {t} GROUP BY {self.key_names[t]} HAVING count(*) > 1;")
            if len(records) > 0:
                self.log.error(f"Data quality check failed for {t}.")
                raise ValueError(f"Data quality check failed for {t}.")
            self.log.info(f"Data quality check passed for {t}.")