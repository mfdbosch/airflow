import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
 
dag = DAG(
   dag_id="print_context",
   start_date=pendulum.today('UTC').add(days=-3),
   schedule="@daily",
   tags=["Loginet"]
)
 
 
def _print_context(**kwargs):
    for key,value in kwargs.items():
        print(f'{key}:{value}\n')
 
 
print_context = PythonOperator(
   task_id="print_context",
   python_callable=_print_context,
   dag=dag,
)