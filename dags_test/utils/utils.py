from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

import db.core as db_core


def dag_el(dag_name, conn_id_source, conn_id_dest, l_tables, default_args, **kwargs):
    """
    For each table in l_tables call dummy_operation and then assign to completed
    :param dag_name: name of your dag
    :type dag_name: str
    :param conn_id_source: connection ID name for source
    :type conn_id_source: str
    :param conn_id_dest: connection ID name for destination
    :type conn_id_dest: str
    :param l_tables: list of table names, the names have to be defined in the both schemas
    :type l_tables: list
    :param default_args: default args
    :type default_args: dict
    :param kwargs: additional keyword arguments for DAG
    :return: DAG object
    """
    # init DAG. The name is important! As it is the one that will figure out in the Flask web page
    # catchup = False -> will NOT create a dag run for each time it should have started from start_date
    dag = DAG(dag_name, default_args=default_args, **kwargs)

    completed = DummyOperator(task_id="complete", dag=dag)  # on_success_callback=slack.slack_succeeded_task)

    # for each table, create a Python operator that calls 'extract_load_table' and set downstream to 'completed' task
    for s_table in l_tables:
        op_el = PythonOperator(
            task_id='extract_and_load_{}'.format(s_table),
            op_kwargs={'table_name': s_table, "conn_id_extract": conn_id_source, "conn_id_load": conn_id_dest},
            python_callable=db_core.dummy_operation,
            dag=dag)

        op_el >> completed

    return dag


# Dag is returned by a factory method
def get_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
    """
    Creates a DAG prepared to be a subdag by convention.
    :param parent_dag_name: parent DAG name
    :type parent_dag_name: str
    :param child_dag_name: this DAG's name
    :type child_dag_name: str
    :param start_date: Parent DAG start_date
    :type start_date: datetime
    :param schedule_interval: Parent DAG schedule_interval
    :type schedule_interval: timedelta
    :return: DAG object
    """
    dag = DAG(
        '{}.{}'.format(parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    start_op = DummyOperator(
        task_id='start_task',
        dag=dag,
    )

    end_task = DummyOperator(
        task_id='dummy_task',
        dag=dag,
    )

    for i in range(3):
        dummy_operator = DummyOperator(
            task_id='dummy_task_num_{}'.format(i),
            dag=dag,
        )

        start_op >> dummy_operator >> end_task

    return dag
