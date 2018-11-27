from datetime import datetime
import logging

from airflow.models import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator


def conditionally_trigger(context, dag_run_obj):
    """This function decides whether or not to Trigger the remote DAG"""
    c_p = context['params']['condition_param']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    if context['params']['condition_param']:
        dag_run_obj.payload = {'message': context['params']['message']}
        logging.info(dag_run_obj.payload)
        return dag_run_obj


# Define the DAG
dag = DAG(
    dag_id='example_trigger_controller_dag',
    default_args={
        "owner": "airflow",
        "start_date": datetime.utcnow(),
    },
    schedule_interval='@once',
)

# Define the single task in this controller example DAG
trigger = TriggerDagRunOperator(
    task_id='test_trigger_dagrun',
    trigger_dag_id="example_trigger_target_dag",
    python_callable=conditionally_trigger,
    params={'condition_param': True, 'message': 'Hello World'},
    dag=dag,
)