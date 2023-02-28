#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.apache.org/tutorial.html)
"""
from __future__ import annotations

# [START tutorial]
# [START import_module]
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.utils.dates import days_ago
import pendulum
import yfinance as yf
import tushare as ts
import akshare as ak
import os

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.operators.python import PythonOperator

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.models import Variable

from airflow.operators.email import EmailOperator

# [END import_module]

os.environ["no_proxy"]="*"
def download_price(**context):
    stock_list = Variable.get("stock_list_json", deserialize_json=True)

    stocks = context["dag_run"].conf.get('stocks')
    print(stocks)
    if stocks:
        stock_list = stocks

    for sy in stock_list:
        stock_zh_b_daily_qfq_df = ak.stock_zh_b_daily(symbol=sy, start_date="20101103", end_date="20201116",
                                              adjust="qfq")
        with open(f'/Users/jiazhenwang/workspace/airflow/logs/{sy}.csv','w') as writer:
            stock_zh_b_daily_qfq_df.to_csv(writer, index=True)
        print(f"Finished downloading price data for {sy}")

    

# download_price()

default_args = {
    'owner' : 'wjz',
    'email': ['jwang43@logitech.com','948151143@qq.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

# [START instantiate_dag]
with DAG(
    dag_id="Download_Stock_Price",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args=default_args,
    # [END default_args]
    description="Download stock price and save to local csv files.",
    # schedule_interval='5 5 * * *',
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["wjz's task"],
) as dag:
    # [END instantiate_dag]
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    download_task = PythonOperator(
        task_id = "download_prices",
        python_callable=download_price
    )
    email_task = EmailOperator(
        task_id='send_email',
        to='jwang43@logitech.com',
        subject='Stock Price is downloaded - {{ds}}',
        html_content=""" <h3>Email Test</h3> {{ ds_nodash }}<br/>{{ dag }}<br/>{{ conf }}<br/>{{ next_ds }}<br/>{{ yesterday_ds }}<br/>{{ tomorrow_ds }}<br/>{{ execution_date }}<br/>""",
        dag=dag
    )

    download_task >>email_task

 


# [END tutorial]