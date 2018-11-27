from airflow.operators.slack_operator import SlackAPIPostOperator
import logging


def slack_failed_task(context_dictionary, **kwargs):
    """
    Connects to Slack and sends a failed message with the table, the timestamp the task failed and params
    :param context_dictionary: Airflow keyword arguments (see all here: https://airflow.apache.org/code.html#macros)
    :return: Returns the execution for the failed function
    """
    try:
        from config.secret import token_slack
    except ModuleNotFoundError:
        logging.warning(
            "There is no secret.py file inside dags/config/ with token_auth defined! No messages will be send to Slack")
        return

    # unpack context
    s_dag = context_dictionary['dag'].dag_id
    timestamp = context_dictionary['ts']
    params = context_dictionary["params"]
    s_task = context_dictionary["task"]

    failed_alert = SlackAPIPostOperator(
        task_id='Failure',
        channel="#airflow-test",
        token=token_slack,
        text=":red_circle: DAG '{}'\nTask: '{}'\n Timestamp: {}\nParams '{}'".format(s_dag, s_task, timestamp, params),
        username="airflow-server",
        owner='_owner', )

    return failed_alert.execute()


def slack_succeeded_task(context_dictionary, **kwargs):
    """
    Connects to Slack and sends a success message with the table, the timestamp the task failed and params
    :param context_dictionary: Airflow keyword arguments (see all here: https://airflow.apache.org/code.html#macros)
    :return: Returns the execution for the passed function
    """

    try:
        from config.secret import token_slack
    except ModuleNotFoundError:
        logging.warning(
            "There is no secret.py file inside dags/config/ with token_auth defined! No messages will be send to Slack")
        return

    # unpack context
    s_dag = context_dictionary['dag'].dag_id
    s_task = context_dictionary["task"]

    succeed_msg = SlackAPIPostOperator(
        task_id='Success',
        channel="#airflow-test",
        token=token_slack,
        text=":green_heart: Completed task {} from DAG '{}'".format(s_task, s_dag),
        username="airflow-server",
        owner='_owner', )

    return succeed_msg.execute()
