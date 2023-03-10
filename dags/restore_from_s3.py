from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.email import EmailOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.decorators import task
from airflow.utils.edgemodifier import Label
from airflow.models import Variable

import datetime as dt
from subprocess import call
from pathlib import Path
import os
from loguru import logger
import ast

os.environ["no_proxy"] = "*"

def _utc_to_cts(time_utc):
    utc_format = "%Y-%m-%dT%H:%M:%S.%f+00:00"
    cts_format = "%Y-%m-%d"
    cts_nodash_format = "%Y%m%d"
    time_cts = dt.datetime.strptime(time_utc, utc_format)+dt.timedelta(hours=8)
    time_cts_timestamp = dt.datetime.timestamp(dt.datetime.strptime(
        dt.datetime.strftime(time_cts, cts_format), cts_format))
    time_cts_nodash = dt.datetime.strftime(time_cts, cts_nodash_format)
    return time_cts, int(time_cts_timestamp), time_cts_nodash

def _generate_local_path(start_date):
    time_cts_nodash = _utc_to_cts(start_date)[2]
    # base_local_dir = '/Users/jiazhenwang/Downloads'
    base_local_dir = Variable.get('base_local_dir')
    local_path = base_local_dir+'/restore/'+time_cts_nodash
    Path(local_path).parent.mkdir(exist_ok=True)
    return local_path

def _download_files_s3(aws_conn_id,start_date,bucket_name,**context):
    db = ast.literal_eval(Variable.get('db_collection_rbd'))['db']
    time_cts_nodash = _utc_to_cts(start_date)[2]
    local_path = os.path.join(_generate_local_path(start_date),db)
    exist_files = []
    for root,dirs,files in os.walk(local_path):
        for f in files:
            exist_file = os.path.join(root,f)
            exist_files.append(exist_file)
    print(exist_files)
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    prefix_s3_key = f'{time_cts_nodash}/{db}/'
    keys = s3_hook.list_keys(bucket_name=bucket_name,prefix=prefix_s3_key,delimiter='/')
    for key in keys:
        dest_file_path = os.path.join(local_path,key.split('/')[-1])
        if dest_file_path in exist_files:
            print(f'????????????????????????????????????:{dest_file_path}???')
            os.remove(dest_file_path)
            print(f'??????????????????????????????:{dest_file_path}???')
        file_name = s3_hook.download_file(key,bucket_name,local_path,True,False)       
        print(f'??????????????????:{file_name}')

# _download_files_s3('my_s3','2023-02-08T01:46:46.338212+00:00','Logitech','wjzawsbucket')
def _restore_data(mongo_conn_id,start_date):
    db = ast.literal_eval(Variable.get('db_collection_rbd'))['db']
    col = ast.literal_eval(Variable.get('db_collection_rbd'))['collection']
    hook = MongoHook(conn_id=mongo_conn_id)
    uri = hook.uri
    uri = uri[:-6]
    local_path = _generate_local_path(start_date)
    restore_cmd = """mongorestore --uri=%s --authenticationDatabase=admin --nsInclude=%s.%s --nsFrom=%s.%s --nsTo=%s.%s %s/""" % (uri,db,col,db,col,'Logitech3','RawDataBucket',local_path)
    print(restore_cmd)
    call(restore_cmd, shell=True)

# _restore_data('my_mongodb','2023-02-08T01:46:46.338212+00:00','Logitech','RawDataBucket')

default_args = {
    'owner': 'wjz',
    'email': ['jwang43@logitech.com', '948151143@qq.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(seconds=30),
}

dag = DAG(
    dag_id="data_restore_lifecycle",
    default_args=default_args,
    start_date=dt.datetime(year=2023, month=2, day=1),
    end_date=dt.datetime(year=2023, month=3, day=11),
    schedule=dt.timedelta(hours=1),
    catchup=False,
    tags=["Loginet",'restore']
)

@task.branch(dag=dag, task_id='check_data')
def _check_data(**context):
    source_id = Variable.get('source_mongo_conn_id')
    target_id = Variable.get('target_mongo_conn_id')
    db = ast.literal_eval(Variable.get('db_collection_rbd'))['db']
    col = ast.literal_eval(Variable.get('db_collection_rbd'))['collection']
    print(str(context['data_interval_start']))
    print(type(context['data_interval_start']))
    start_timestamp = _utc_to_cts(str(context['data_interval_start']))[1]
    print(f'start_timestamp:{start_timestamp}')
    items = []
    mongo_query = {'timestamp': {'$eq': 1675906110000}}
    source_db = MongoHook(conn_id=source_id)
    target_db = MongoHook(conn_id=target_id)
    source_col = source_db.get_collection(col, db)
    # Logitech3 ???????????????db
    target_col = target_db.get_collection(col, 'Logitech3')
    source_count = source_col.count_documents(mongo_query)
    target_count = target_col.count_documents(mongo_query)
    if not source_count == target_count:
        return 'download_files_s3'
    else:
        return 'no_action'

download_files_s3 = PythonOperator(
    task_id = 'download_files_s3',
    python_callable=_download_files_s3,
    op_kwargs={
        'aws_conn_id':'{{var.value.aws_conn_id}}',
        'start_date':'{{data_interval_start}}',
        'bucket_name':'{{var.value.s3_bucket_name}}'
    },
    dag= dag
)

restore_data = PythonOperator(
    task_id ='restore_data',
    python_callable=_restore_data,
    op_kwargs={
        'mongo_conn_id':'{{var.value.target_mongo_conn_id}}',
        'start_date':'{{data_interval_start}}'
    },
    dag=dag
)

send_success_email = EmailOperator(
    task_id='send_success_email',
    to='jwang43@logitech.com',
    subject='restore_data - {{ds}}',
    html_content=""" <h3>Success!</h3> {{ ds_nodash }}<br/>{{ dag }}<br/>{{ conf }}<br/>{{ data_interval_start }}<br/>{{ data_interval_end }}<br/>{{ ts_nodash_with_tz }}<br/>{{ ts_nodash }}<br/>{{prev_start_date_success}}<br/>""",
    dag=dag
)

no_action = EmailOperator(
    task_id='no_action',
    to='jwang43@logitech.com',
    subject='no data need to process- {{ds}}',
    html_content=""" <h3>no data need to process!</h3> {{ ds_nodash }}<br/>{{ dag }}<br/>{{ conf }}<br/>{{ data_interval_start }}<br/>{{ data_interval_end }}<br/>{{ ts_nodash_with_tz }}<br/>{{ ts_nodash }}<br/>{{prev_start_date_success}}<br/>""",
    dag=dag
)

check_data = _check_data()
check_data >> Label("Need restore data") >> download_files_s3 >> restore_data >> send_success_email
check_data >> Label("no data need to process") >> no_action