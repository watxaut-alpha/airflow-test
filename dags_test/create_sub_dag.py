# main_dag.py
from datetime import datetime
from airflow.models import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator

from utils.utils import get_sub_dag

PARENT_DAG_NAME = 'parent_dag'
CHILD_DAG_NAME_1 = 'child_dag_1'
CHILD_DAG_NAME_2 = 'child_dag_2'

main_dag = DAG(
    dag_id=PARENT_DAG_NAME,
    schedule_interval="@once",
    start_date=datetime(2018, 11, 21)
)

start_op = DummyOperator(
    task_id='start',
    dag=main_dag,
)

sub_dag = SubDagOperator(
    subdag=get_sub_dag(PARENT_DAG_NAME, CHILD_DAG_NAME_1, main_dag.start_date,
                       main_dag.schedule_interval),
    task_id=CHILD_DAG_NAME_1,
    dag=main_dag,
)

sub_dag_2 = SubDagOperator(
    subdag=get_sub_dag(PARENT_DAG_NAME, CHILD_DAG_NAME_2, main_dag.start_date,
                       main_dag.schedule_interval),
    task_id=CHILD_DAG_NAME_2,
    dag=main_dag,
)

end_op = DummyOperator(
    task_id='end',
    dag=main_dag,
)

middle_task = DummyOperator(
    task_id='middle_operation',
    dag=main_dag,
)

start_op >> sub_dag >> middle_task >> sub_dag_2 >> end_op
