from __future__ import annotations

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.email import EmailOperator

import datetime as dt

def create_dag(dag_id,schedule,default_args):
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        start_date=dt.datetime(year=2023, month=2, day=1),
        end_date=dt.datetime(year=2023, month=3, day=16),
        schedule=schedule,
        catchup=False,
        tags=['Pentaho','SSH','test']
    )

    ssh_task = SSHOperator(
        task_id = 'ssh_task',
        ssh_conn_id= 'ssh_pentaho',
        command= 'sh /data/job/test.sh ',
        get_pty=True,
        cmd_timeout= 300,
        do_xcom_push = False,
        dag = dag
    )

    email_task = EmailOperator(
        task_id='send_email',
        to='jwang43@logitech.com',
        subject='{{dag.dag_id}} - {{ds}}',
        html_content=""" <h3>Success!</h3>""",
        dag=dag
    )

    ssh_task >> email_task
    
    return dag

default_args = {
    'owner': 'wjz',
    'email': ['jwang43@logitech.com', '948151143@qq.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': dt.timedelta(seconds=30),
}

schedule_list=['40 6 * * *','30 17 * * *']

for i in range(len(schedule_list)):
    dag_id = f'test_task{i}'
    schedule = schedule_list[i]
    globals()[dag_id] = create_dag(dag_id,schedule,default_args)