"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/incubator-airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from config.secret import db_password
import time
import MySQLdb


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 8),
    'email': ['watxaut@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('test_airflow_python', default_args=default_args)


def connect_to_db():
    db = MySQLdb.connect(host="localhost",  # your host, usually localhost
                         user="airflow",  # your username
                         passwd=db_password,  # your password
                         db="airflow-db")  # name of the data base

    # you must create a Cursor object. It will let
    #  you execute all the queries you need
    cur = db.cursor()

    return db, cur


def sleep(sleep_time):
    """This is a function that will run within the DAG execution"""
    time.sleep(sleep_time)


def function_operation(sleep_time, s_operation):

    # fetch something from the DB here for example
    # for now, only wait some time depending on the operation
    sleep(sleep_time)
    return "Waited {wait_time}. Operation {operation} has been executed".format(wait_time=sleep_time, operation=s_operation)


o1 = PythonOperator(
    task_id='first_op',
    op_kwargs={'sleep_time': 5, 's_operation': 'Operation 1'},
    python_callable=function_operation,
    dag=dag)

o2 = PythonOperator(
    task_id='second_op',
    op_kwargs={'sleep_time': 3, 's_operation': 'Operation 2'},
    python_callable=function_operation,
    dag=dag)

o3 = PythonOperator(
    task_id='third_op',
    op_kwargs={'sleep_time': 4, 's_operation': 'Operation 3'},
    python_callable=function_operation,
    dag=dag)


o3.set_upstream(o2)
o2.set_upstream(o1)
