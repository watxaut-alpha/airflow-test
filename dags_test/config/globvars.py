from datetime import datetime, timedelta
import slack.msg as slack

# max number of rows allowed inside a pandas df. Pandas will process the query and only extract this many rows at a time
# in a iterator. Successive calls in this iterator return a pandas df with this many rows until finished
max_num_rows_to_df = 25000


# Airflow's DAG default arguments for DAG. You can see a complete list
# here: https://airflow.apache.org/code.html#airflow.models.BaseOperator
default_args = {
    'owner': 'watxaut_dev',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 15),
    'email': ['watxaut@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,  # number of retries
    'retry_delay': timedelta(seconds=20),  # retry timing
    'on_failure_callback': slack.slack_failed_task,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
}