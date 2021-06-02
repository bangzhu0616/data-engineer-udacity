from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_template = """
             COPY {} 
             FROM '{}' 
             ACCESS_KEY_ID '{}' 
             SECRET_ACCESS_KEY '{}'
             REGION '{}'
             {};
            """
    csv_format = "CSV IGNOREHEADER 1"
    json_format = "FORMAT AS JSON 'auto'"

    @apply_defaults
    def __init__(self,
                 aws_credentials_id='',
                 redshift_conn_id='',
                 s3_bucket='',
                 s3_key='',
                 region='',
                 table='',
                 file_format='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.file_format = file_format
        self.table = table

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        self.log.info(f"Staging file location {s3_path}")
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if self.file_format == 'CSV':
            s3_copy_query = self.copy_template.format(self.table, s3_path, credentials.access_key, credentials.secret_key, self.region, self.csv_format)
        if self.file_format == 'JSON':
            s3_copy_query = self.copy_template.format(self.table, s3_path, credentials.access_key, credentials.secret_key, self.region, self.json_format)
        redshift.run(s3_copy_query)
        self.log.info(f"Table {self.table} staged!")
