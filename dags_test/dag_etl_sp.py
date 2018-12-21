from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator

import db.core as db_core

from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime.utcnow(),
}

l_extract = ['dummy_table',
             'dummy_table2',
             'dummy_table3',
             'dummy_table4',
             ]

l_load = ['dummy_table',
          'dummy_table2',
          'dummy_table3',
          'dummy_table4',
          ]

dag = DAG("dag_etl_sp", schedule_interval="@once", default_args=default_args)

conn_id_extract = "conn_test_source"
conn_id_transform = "conn_test_transform"
conn_id_load = "conn_test_load"

o_transform_sp = MySqlOperator(
    task_id='transform_sp',
    sql='call test_transform.dummy_airflow_sp()',
    mysql_conn_id=conn_id_transform,
    dag=dag
)

o_truncate_transform = MySqlOperator(
    task_id='truncate_transform',
    sql='call test_transform.truncate_dummy_data()',
    mysql_conn_id=conn_id_transform,
    dag=dag
)

o_truncate_load = MySqlOperator(
    task_id='truncate_load',
    sql='call test_destination.truncate_dummy_data()',
    mysql_conn_id=conn_id_load,
    dag=dag
)

o_completed = DummyOperator(
    task_id="Completed",
    dag=dag

)

o_truncate_transform >> o_truncate_load

for table in l_extract:
    o_extract = PythonOperator(
        task_id='extract_from_{}'.format(table),
        op_kwargs={'table_name': table, "conn_id_extract": conn_id_extract, "conn_id_load": conn_id_transform,
                   "load_all": True},
        python_callable=db_core.extract_load_table,
        dag=dag)

    o_truncate_load >> o_extract >> o_transform_sp

for table in l_load:
    o_load = PythonOperator(
        task_id='load_to_{}'.format(table),
        op_kwargs={'table_name': table, "conn_id_extract": conn_id_transform, "conn_id_load": conn_id_load,
                   "load_all": True},
        python_callable=db_core.extract_load_table,
        dag=dag)

    o_transform_sp >> o_load >> o_completed
