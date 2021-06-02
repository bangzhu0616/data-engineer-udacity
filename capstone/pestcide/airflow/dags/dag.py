from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (CreateTableOperator, StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, DataKeyCheckOperator)
from helpers import SqlQueries

s3_bucket='pestcidedb-capstone'

default_args = {
    'owner': 'bangzhu',
    'start_date': datetime(2021, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'depends_on_past': False,
    'catchup': False,
}

dag = DAG('pestcidedb_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_in_redshift = CreateTableOperator(
    task_id = 'create_tables_in_redshift',
    dag = dag,
    redshift_conn_id = 'redshift'
)

stage_pestcidedb_to_redshift = StageToRedshiftOperator(
    task_id='stage_pestcidedb',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    s3_bucket=s3_bucket,
    s3_key='pestcide',
    region='us-west-2',
    file_format='CSV',
    table='staging_pestcide'
)

stage_dictionary_to_redshift = StageToRedshiftOperator(
    task_id='stage_dictionary',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    s3_bucket=s3_bucket,
    s3_key='dictionary',
    region='us-west-2',
    file_format='JSON',
    table='staging_dictionary'
)

load_state_dimension_table = LoadDimensionOperator(
    task_id='Load_state_dim_table',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.state_table_insert,
    delete_data=True,
    table='states'
)

load_county_dimension_table = LoadDimensionOperator(
    task_id='Load_county_dim_table',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.county_table_insert,
    delete_data=True,
    table='counties'
)

load_compound_dimension_table = LoadDimensionOperator(
    task_id='Load_compound_dim_table',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.compound_table_insert,
    delete_data=True,
    table='compounds'
)

load_pestcide_usage_table = LoadFactOperator(
    task_id='Load_pestcide_usage_fact_table',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.pestcide_usage_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    tables=['pestcide_use', 'compounds', 'counties', 'states']
)

run_uniquekey_checks = DataKeyCheckOperator(
    task_id='Run_data_uniquekey_checks',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    tables=['compounds', 'counties', 'states']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_in_redshift
create_tables_in_redshift >> [stage_pestcidedb_to_redshift, stage_dictionary_to_redshift]
stage_dictionary_to_redshift >> load_state_dimension_table
load_state_dimension_table >> load_county_dimension_table
stage_pestcidedb_to_redshift >> load_compound_dimension_table
[load_county_dimension_table, load_compound_dimension_table] >> load_pestcide_usage_table
load_pestcide_usage_table >> [run_quality_checks, run_uniquekey_checks]
[run_quality_checks, run_uniquekey_checks] >> end_operator
