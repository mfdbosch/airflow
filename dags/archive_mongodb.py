from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.email import EmailOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.edgemodifier import Label
from airflow.decorators import task

import datetime as dt
from loguru import logger
from subprocess import call
from pathlib import Path
import os

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


def _fetch_data(source_id, target_id, col, db, batch, start_date):
    start_timestamp = _utc_to_cts(start_date)[1]
    print(start_timestamp)
    items = []
    mongo_query = {'timestamp': {'$eq': 1675906110000}}
    source_db = MongoHook(conn_id=source_id)
    target_db = MongoHook(conn_id=target_id)
    source_count = source_db.get_collection(
        col, db).count_documents(mongo_query)
    print(f"共有{source_count}条记录需更新！")
    target_db.delete_many(col, mongo_query, 'Logitech2')
    flag = 0
    for i in source_db.find(col, mongo_query, False, db, None):
        items.append(i)
        flag += 1
        if flag % batch == 0 or flag == source_count:
            target_db.insert_many(col, items, 'Logitech2')
            print(f'数据库 {db} {col}已更新{flag}条')
            items = []
    print(f"共有{source_count}条记录更新成功！")
    source_db.close_conn()
    target_db.close_conn()


def _dump_data(mongo_conn_id, start_date, db, col):
    hook = MongoHook(conn_id=mongo_conn_id)
    uri = hook.uri
    print(f'{uri}')
    uri = uri[:-6]
    timestamp1 = 1675906110000
    time_cts_timestamp = _utc_to_cts(start_date)[1]
    print(time_cts_timestamp)
    base_dir = '/Users/jiazhenwang/Downloads/dump'
    time_cts_nodash = _utc_to_cts(start_date)[2]
    dump_dir = base_dir+'/'+time_cts_nodash
    Path(dump_dir).parent.mkdir(exist_ok=True)
    dump_cmd = """mongodump --uri=%s --authenticationDatabase=admin -d=%s -c=%s -q='{"timestamp": {"$eq": %s}}' -o=%s""" % (
        uri, db, col, timestamp1, dump_dir)
    print(dump_cmd)
    call(dump_cmd, shell=True)


def _upload_files_s3(aws_conn_id, start_date, db, **context):
    # start_time = f'2023-02-08T01:46:46.338212+00:00'
    time_cts_nodash = _utc_to_cts(start_date)[2]
    base_dir = '/Users/jiazhenwang/Downloads/dump'
    dump_dir = base_dir+'/'+time_cts_nodash
    print(dump_dir)
    Path(dump_dir).parent.mkdir(exist_ok=True)
    for root, dirs, files in os.walk(dump_dir):
        for f in files:
            if f != '.DS_Store':
                dumpfile = os.path.join(root, f)
                LocalFilesystemToS3Operator(
                    task_id='upload_s3',
                    filename=dumpfile,
                    dest_key=f'{time_cts_nodash}/{db}/{f}',
                    dest_bucket='wjzawsbucket',
                    aws_conn_id=aws_conn_id,
                    replace=True).execute(context)
                print(f'成功上传{dumpfile}')


default_args = {
    'owner': 'wjz',
    'email': ['jwang43@logitech.com', '948151143@qq.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': dt.timedelta(seconds=30),
}

dag = DAG(
    dag_id="data_archival_lifecycle",
    default_args=default_args,
    start_date=dt.datetime(year=2023, month=2, day=1),
    end_date=dt.datetime(year=2023, month=2, day=14),
    schedule=dt.timedelta(minutes=10),
    catchup=False,
    tags=["Loginet",'dump']
)


fetch_data = PythonOperator(
    task_id='fetch_data',
    python_callable=_fetch_data,
    op_kwargs={
        'source_id': 'my_mongodb',
        'target_id': 'my_mongodb',
        'col': 'RawDataBucket',
        'db': 'Logitech',
        'batch': 50,
        'start_date': '{{data_interval_start}}'
    },
    dag=dag
)


@task.branch(dag=dag, task_id='check_data')
def _check_data(source_id, target_id, col, db, **context):
    print(str(context['data_interval_start']))
    print(type(context['data_interval_start']))
    start_timestamp = _utc_to_cts(str(context['data_interval_start']))[1]
    print(start_timestamp)
    items = []
    mongo_query = {'timestamp': {'$eq': 1675906110000}}
    source_db = MongoHook(conn_id=source_id)
    target_db = MongoHook(conn_id=target_id)
    source_col = source_db.get_collection(col, db)
    # Logitech2 需要替换成db
    target_col = target_db.get_collection(col, 'Logitech2')
    source_count = source_col.count_documents(mongo_query)
    target_count = target_col.count_documents(mongo_query)
    if not source_count == target_count:
        return 'fetch_data'
    else:
        return 'no_action'


dump_data = PythonOperator(
    task_id='dump_data',
    python_callable=_dump_data,
    op_kwargs={
        'mongo_conn_id': 'my_mongodb',
        'start_date': '{{data_interval_start}}',
        'db': 'Logitech',
        'col': 'RawDataBucket'
    },
    dag=dag
)

upload_files_s3 = PythonOperator(
    task_id='upload_files_s3',
    python_callable=_upload_files_s3,
    op_kwargs={
        'aws_conn_id': 'my_s3',
        'start_date': '{{data_interval_start}}',
        'db': 'Logitech',
    },
    dag=dag
)
email_task2 = EmailOperator(
    task_id='send_email',
    to='jwang43@logitech.com',
    subject='archive_data - {{ds}}',
    html_content=""" <h3>Success!</h3> {{ ds_nodash }}<br/>{{ dag }}<br/>{{ conf }}<br/>{{ data_interval_start }}<br/>{{ data_interval_end }}<br/>{{ ts_nodash_with_tz }}<br/>{{ ts_nodash }}<br/>{{prev_start_date_success}}<br/>""",
    dag=dag
)

email_task1 = EmailOperator(
    task_id='no_action',
    to='jwang43@logitech.com',
    subject='no data need to process- {{ds}}',
    html_content=""" <h3>no data need to process!</h3> {{ ds_nodash }}<br/>{{ dag }}<br/>{{ conf }}<br/>{{ data_interval_start }}<br/>{{ data_interval_end }}<br/>{{ ts_nodash_with_tz }}<br/>{{ ts_nodash }}<br/>{{prev_start_date_success}}<br/>""",
    dag=dag
)


check_data = _check_data('my_mongodb', 'my_mongodb',
                         'RawDataBucket', 'Logitech')
check_data >> [fetch_data, email_task1]
fetch_data >> dump_data >> upload_files_s3 >> email_task2
