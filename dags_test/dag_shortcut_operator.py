from airflow.models import DAG
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import datetime
import logging


def check_hour(l_exclude_hours, *args, **kwargs):
    execution_date = str(kwargs['execution_date']).split(".")[0]  # get rid of milliseconds
    execution_date = datetime.strptime(execution_date, "%Y-%m-%dT%H:%M:%S")

    for exclude_tuple in l_exclude_hours:
        exclude_date = datetime(execution_date.year, execution_date.month, execution_date.day, exclude_tuple[0],
                                exclude_tuple[1], exclude_tuple[2])

        calc_seconds = (exclude_date-execution_date).total_seconds()
        logging.info("Seconds until  '{}': '{}'".format(exclude_date, calc_seconds))

        if calc_seconds < seconds_boundary:

            # search for others!
            continue
        else:

            # there is already one time it does not meet. Stop
            return False

    else:
        return True


# if except hour is at 24h OJU, this will break
l_exclude_hours = [(2, 0, 0), (6, 30, 0), (18, 30, 0)]
seconds_boundary = 60 * 20  # 20 minutes from side to side

default_args = {
    "owner": "airflow",
    "start_date": datetime.utcnow(),
}

dag = DAG("dag_shortcut_operator", schedule_interval="@once", default_args=default_args)

op_shortcut = ShortCircuitOperator(
    task_id="ShortCut_operation",
    python_callable=check_hour,
    op_kwargs={'l_exclude_hours': l_exclude_hours},
    provide_context=True,
    dag=dag
)

op1 = DummyOperator(
    task_id="Op1",
    dag=dag

)

op2 = DummyOperator(
    task_id="Op2",
    dag=dag
)

op1 >> op_shortcut >> op2
