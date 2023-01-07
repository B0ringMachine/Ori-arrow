from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from astro import sql as aql
from astro.file import File
from astro.sql.table import Table

S3_File_Path = "s3://imba-carl/data"
S3_CONN_ID = "aws_default"
SNOWFLAKE_CONN_ID = 'snowflake_default'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023,1,1),
    'retries': 0
}

with DAG('ETL', default_args=default_args, schedule_interval='@daily') as dag:
    task_1 = aql.load_file(
        input_file_1=File(
            path=S3_File_Path + '/aisles/aisles.csv', conn_id=S3_CONN_ID
        ),
        output_table_1=Table(conn_id=SNOWFLAKE_CONN_ID),
    
        input_file_2=File(
            path=S3_File_Path + '/departments/departments.csv', conn_id=S3_CONN_ID
        ),
        output_table_2=Table(conn_id=SNOWFLAKE_CONN_ID),

        input_file_3=File(
            path=S3_File_Path + '/order_products/order_products__prior.csv', conn_id=S3_CONN_ID
        ),
        output_table_3=Table(conn_id=SNOWFLAKE_CONN_ID),

        input_file_4=File(
            path=S3_File_Path + '/orders/orders.csv', conn_id=S3_CONN_ID
        ),
        output_table_4=Table(conn_id=SNOWFLAKE_CONN_ID),

        input_file_5=File(
            path=S3_File_Path + '/products/products.csv', conn_id=S3_CONN_ID
        ),
        output_table_5=Table(conn_id=SNOWFLAKE_CONN_ID),

        task_id='LOAD_S3_TO_SNOWFLAKE',

        dag=dag
        )


    task_2 = BashOperator(
        task_id='TRANSFORM',
        bash_command='cd /mnt/c/Users/czcch/ori_arrow && dbt run --models --profiles-dir.',
        dag=dag)

    hook = S3Hook(aws_conn_id="aws_default")
    hook.load_file(
        filename='/usr/local/airflow/data.csv',
        key='caloschen.csv',
        bucket_name=Variable.get("Processed_Data"),
        replace=True,
    )

    task_3 = PythonOperator(task_id='Load_to_S3',     
                             python_callable=snowflake_to_pandas,
                             op_kwargs={"query":query,"snowflake_conn_id":'snowflake_default'},     
                             dag=dag )
    
task_1>>task_2>>task_3