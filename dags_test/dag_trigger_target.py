from datetime import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

args = {
    'start_date': datetime.utcnow(),
    'owner': 'airflow',
}

dag = DAG(
    dag_id='example_trigger_target_dag',
    default_args=args,
    schedule_interval=None,
)


def run_this_func(ds, **kwargs):
    print("Remotely received value of {} for key=message".
          format(kwargs['dag_run'].conf['message']))


run_this = PythonOperator(
    task_id='run_this',
    provide_context=True,
    python_callable=run_this_func,
    dag=dag,
)
