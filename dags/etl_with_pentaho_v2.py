from __future__ import annotations

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import datetime as dt
import os

os.environ["no_proxy"] = "*"

default_args = {
    'owner': 'wjz',
    'email': ['jwang43@logitech.com', '948151143@qq.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': dt.timedelta(seconds=30),
}

def _operate_data(**context):
    data_lists = context['ti'].xcom_pull(task_ids ='select_data')
    print(len(data_lists))
    print(data_lists)
    print(type(data_lists))


dag = DAG(
    dag_id="airflow_with_mysql",
    default_args=default_args,
    start_date=dt.datetime(year=2023, month=2, day=1),
    end_date=dt.datetime(year=2023, month=2, day=17),
    schedule=dt.timedelta(minutes=30),
    catchup=False,
    tags=['Pentaho','MySQL']
)


select_data = MySqlOperator(
    task_id = 'select_data',
    sql = """select * from DeptInfo""",
    mysql_conn_id='epmqacl',
    dag=dag
)

operate_data = PythonOperator(
    task_id = 'operate_data',
    python_callable=_operate_data,
    dag =dag
)

select_data >> operate_data

