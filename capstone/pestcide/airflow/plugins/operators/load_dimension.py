from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id='',
                 redshift_conn_id='',
                 sql_query='',
                 delete_data=False,
                 table='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.delete_data = delete_data
        self.table=table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.delete_data and self.table:
            redshift.run(f"DELETE FROM {self.table}")
        redshift.run(self.sql_query)

