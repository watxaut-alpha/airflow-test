from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta

from config.v1.table_definitions import tables_test
import slack.msg as slack
from utils import utils


# set up DAG
dag_name = "dag_ETL_template"
dag_args = {}
schedule_interval = "@once"
concurrency = 4
max_active_runs = 1

# extract the information from the config.py file
conn_id_extract = "conn_test_source"
conn_id_transform = "conn_test_transform"
conn_id_load = "conn_test_load"
l_tables = tables_test
db_version = "v1"


# DAG OBJECT
dag = utils.dag_constructor(dag_name, dag_args, catchup=False,
                            schedule_interval=schedule_interval, max_active_runs=max_active_runs,
                            concurrency=concurrency)


# Create a dummy operation just to know when every task is completed
completed = DummyOperator(task_id="complete",
                          dag=dag,
                          on_success_callback=slack.slack_succeeded_task)


d_op_el = utils.get_python_operators_el(dag, conn_id_extract, conn_id_load, l_tables, db_version)

completed.set_upstream(d_op_el.values())
