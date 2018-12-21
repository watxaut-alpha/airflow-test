from datetime import datetime, timedelta

# Airflow's DAG default arguments for DAG. You can see a complete list
# here: https://airflow.apache.org/code.html#airflow.models.BaseOperator
default_args = {
    'owner': 'watxaut',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 15),
    'email': ['watxaut@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,  # number of retries
    'retry_delay': timedelta(seconds=20),  # retry timing
    # 'on_failure_callback': slack.slack_failed_task,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
}
