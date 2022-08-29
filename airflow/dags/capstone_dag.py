from datetime import datetime, timedelta
import os
from airflow import (DAG, conf)
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.operators import (
    DataQualityOperator,
    CreateTablesOperator,
    CopyToRedshiftOperator
)
from helpers import (
    table_list,
    copy_tables
)

default_args = {
    'owner': 'haip pham',
    'start_date': datetime(2022, 7, 23),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('capstone_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = CreateTablesOperator(
    task_id="create_redshift_tables",
    dag=dag,
    redshift_conn_id="redshift",
    create_table_file='create_capstone_tables.sql'
)

# Define list contains operators
copy_table_from_s3=[]
for table in copy_tables:
  copy_table_from_s3.append(
    CopyToRedshiftOperator(
    task_id=f'copy_{table["name"]}_from_s3',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    table=table['name'],
    s3_bucket="datalake-udacity-haipd4",
    region="us-west-2",
    s3_key=table['key'],
    file_format=table['file_format'],
    additional=table['additional']
  ))


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table=table_list
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator  \
    >> create_tables \
    >> copy_table_from_s3 \
    >> run_quality_checks \
    >> end_operator
