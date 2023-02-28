"""
This DAG reads data from an testing API and save the downloaded data into a JSON file.
"""

import yfinance as yf
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.email import EmailOperator
import os
import pendulum

os.environ["no_proxy"]="*"

default_args = {
    'owner' : 'wjz',
    'email': ['jwang43@logitech.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}


with DAG(
    dag_id =  'operate_aws_s3',
    default_args=default_args,
    description='This DAG provies samples operations in AWS S3. ',
    schedule='15 5 * * *',
    start_date=pendulum.today('UTC').add(days=-2),
    tags=["wjz'task"],
    catchup=True,
    max_active_runs=1,
) as dag:
    s3_sensor = S3KeySensor(
        task_id='new_s3_file',
        bucket_key='archive/{{ds_nodash}}/*.csv',
        wildcard_match=True,
        bucket_name='wjzawsbucket',
        aws_conn_id='my_s3',
        timeout=18*60*60,
        poke_interval=30,
        dag=dag)
    list_s3_file = S3ListOperator(
        task_id='list_s3_files',
        bucket='wjzawsbucket',
        prefix='archive/',
        delimiter='/',
        aws_conn_id='my_s3'
    )
    
    trigger_next_dag = TriggerDagRunOperator(
        trigger_dag_id = "Download_Stock_Price",
        task_id = "download_prices_dag",
        execution_date = "{{ds}}",
        # wait_for_completion =False
    )

    # trigger = TriggerDagRunOperator(
    #     task_id="test_trigger_dagrun",
    #     trigger_dag_id="example_trigger_target_dag",  # Ensure this equals the dag_id of the DAG to trigger
    #     conf={"message": "Hello World"},
    # )
    email_task = EmailOperator(
        task_id='send_email',
        to='jwang43@logitech.com',
        subject='operate s3 files DAG - {{ds}}',
        html_content=""" <h3>Email Test</h3> {{ ds_nodash }}<br/>{{ dag }}<br/>{{ conf }}<br/>{{ data_interval_start }}<br/>{{ data_interval_end }}<br/>{{ ts_nodash_with_tz }}<br/>{{ ts_nodash }}<br/>{{prev_start_date_success}}<br/>""",
        dag=dag
    )

    s3_sensor >> list_s3_file >> trigger_next_dag >> email_task