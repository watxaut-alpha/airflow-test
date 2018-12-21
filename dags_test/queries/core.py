import logging

from airflow.hooks.mysql_hook import MySqlHook

import config.v1.query as query_v1


def get_max_updated_at(conn_id, table_name):
    """
    Gets the max updated_at timestamp. If table is empty it returns None
    :param conn_id: Connection ID from the DB to search for
    :type conn_id: str
    :param table_name: name of the table to extract the max updated_at
    :type table_name: str
    :return: str with timestamp or None
    """
    mysql_hook = MySqlHook(conn_id)

    sql_query = "SELECT MAX(updated_at) FROM {}".format(table_name)
    max_updated_at = mysql_hook.get_records(sql_query)[0][0]  # row 0, column 0. Only one record returned

    return max_updated_at


def get_columns_and_exclude(conn_id, table_name, l_columns_exclude):
    """

    :param conn_id: connection id to connect to
    :param table_name: table to get the columns
    :param l_columns_exclude: list of strings of columns to exclude
    :return: list of strings without the excluded columns
    """
    mysql_hook = MySqlHook(conn_id)

    sql_query = "SHOW COLUMNS FROM {}".format(table_name)
    all_records = mysql_hook.get_records(sql_query)

    l_columns_after_exclude = ["t.`{}`".format(l_row[0]) for l_row in all_records if
                               l_row[0] not in l_columns_exclude]
    logging.debug("Columns after exclude: '{}'".format(l_columns_after_exclude))

    return l_columns_after_exclude


def get_query_class(db_version):

    if db_version == "v1":
        return query_v1.QueryConfig()
    else:
        raise Exception("No such DB version: '{}'".format(db_version))


# noinspection PyArgumentList
def create_query(conn_id_source, conn_id_destination, sql_query_params, load_all):

    # unpack info from table
    table_id = sql_query_params["table_id"]
    table_name_source = sql_query_params["table_name_source"]
    table_name_destination = sql_query_params["table_name_destination"]
    l_columns_exclude = sql_query_params["l_columns_exclude"]
    operation_type = sql_query_params["operation_type"]

    # get db version from DAG
    db_version = sql_query_params["db_version"]

    # get which columns to copy
    l_columns = get_columns_and_exclude(conn_id_source, table_name_source, l_columns_exclude)
    s_columns = str(tuple(l_columns)).replace('"', "'").replace("'", "")[1:-1]

    logging.info(
        "table_id: '{}' is operation_type: '{}' with db version: '{}'".format(table_id, operation_type, db_version))

    # get class for each type of db
    query_class = get_query_class(db_version)

    if operation_type is None:  # Normal EL
        f_query = query_class.get_query("base_query", None)
    elif operation_type == "query1":

        # do something with data and call create query1
        query_type = sql_query_params["query_type"]

        f_query = query_class.get_query("query1", query_type)

    elif operation_type == "query2":
        f_query = query_class.get_query("query2", table_name_destination)
    else:
        raise Exception("Found one operation_type that does not correspond to any type: '{}'".format(operation_type))

    base_query = f_query(table_name_source, s_columns)

    # if we want to load all no need to know the max updated_at
    max_timestamp_destination = get_max_updated_at(conn_id_destination, table_name_destination)
    logging.info("Max destination timestamp: '{}'".format(max_timestamp_destination))

    if max_timestamp_destination is None or load_all:
        logging.warning("Copying all table from '{}' to '{}'".format(table_name_source, table_name_destination))
        sql_query = base_query
    else:
        if operation_type != "query2":  # two wheres in the same query are not possible
            sql_query = "{} WHERE t.updated_at > '{}'".format(base_query, max_timestamp_destination)
        else:
            sql_query = "{} AND t.updated_at > '{}'".format(base_query, max_timestamp_destination)

    return sql_query
