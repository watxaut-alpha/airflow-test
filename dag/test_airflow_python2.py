"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/incubator-airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import time
from config.secret import db_password
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


# init DAG. The name is important! As it is the one that will figure out in the Flask web page
dag = DAG('test_airflow_python2', default_args=default_args)


def connect_to_db():
    """
    Returns Airflow-db DB and cursor
    :return:
    """
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

o4 = PythonOperator(
    task_id='forth_op',
    op_kwargs={'sleep_time': 4, 's_operation': 'Operation 4'},
    python_callable=function_operation,
    dag=dag)

o5 = PythonOperator(
    task_id='fifth_op',
    op_kwargs={'sleep_time': 2, 's_operation': 'Operation 5'},
    python_callable=function_operation,
    dag=dag)

o6 = PythonOperator(
    task_id='sixth_op',
    op_kwargs={'sleep_time': 9, 's_operation': 'Operation 6'},
    python_callable=function_operation,
    dag=dag)

o7 = PythonOperator(
    task_id='seventh_op',
    op_kwargs={'sleep_time': 7, 's_operation': 'Operation 7'},
    python_callable=function_operation,
    dag=dag)

o8 = PythonOperator(
    task_id='eighth_op',
    op_kwargs={'sleep_time': 4, 's_operation': 'Operation 8'},
    python_callable=function_operation,
    dag=dag)


# set directed streams from 1 to 2,3,4,5
o1.set_downstream([o2, o3, o4, o5])

# set directed streams from 2,3,4 to 6
o6.set_upstream([o2, o3, o4])

# set stream from 2 to 7
o7.set_upstream(o2)

# set stream from 5,6,7 to 8
o8.set_upstream([o7, o6, o5])


