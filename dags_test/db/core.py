from airflow.hooks.mysql_hook import MySqlHook
import traceback
import pandas as pd
import logging

import db.hooks as hooks
import config.globvars as globvars
import queries.core as query


class DBNotLoadedException(Exception):
    """
    Exception thrown when there is a problem inside the EL process
    """
    pass


def get_pandas_iterator(conn_id_source, sql_query):
    mysql_hook_extract = hooks.MyMysqlHook(conn_id_source)

    logging.debug("Getting iterator of pandas df with chunks of '{}' rows".format(globvars.max_num_rows_to_df))
    logging.debug("Executing query: '{}'".format(sql_query))
    iter_df = mysql_hook_extract.get_pandas_df_iterable(sql_query, globvars.max_num_rows_to_df)
    logging.info("Done getting iterator of df!")

    return iter_df


def load_rows_to_destination(df, conn_id_dest, table_name_dest, load_all):
    # extract every table as a data frame and convert it to object type -> NaT values then are treated as null
    # objects and can be converted to None
    df = df.astype(object)

    # convert nulls to None (needed in MySQL upload)
    logging.debug("Convert NaN, NaT -> None")
    df = df.where(pd.notnull(df), None)

    target_fields = list(df.keys())

    logging.info("Column fields from source: {}".format(target_fields))
    logging.info("Row Count from chunk source: '{}'".format(df.shape[0]))

    if not load_all:  # just load the part that has updated_at > last_destination_updated_at
        mysql_hook_load = hooks.MyMysqlHook(conn_id_dest)

        # replace should be false, but cannot be sure that we are not repeating values
        mysql_hook_load.insert_update_on_duplicate_rows(table_name_dest, rows=df.values.tolist(),
                                                        columns=target_fields,
                                                        commit_every=1000)

    else:  # load everything replacing any value if the same PK is found
        mysql_hook_load = MySqlHook(conn_id_dest)
        mysql_hook_load.insert_rows(table_name_dest, df.values.tolist(), target_fields=target_fields,
                                    commit_every=1000,
                                    replace=True)


def extract_load_table(conn_id_source, conn_id_destination, sql_query_params, load_all):
    """
    Extract all rows that match "updated_at > last_destination_updated_at" or if the destination table is empty
    (updated_at = None) then all table is copied from source and pasted in destination.
    :param conn_id_source: connection id from source (has to match with one in Admin/connections in the web server)
    :type conn_id_source: str
    :param conn_id_destination: connection id from destination (has to match with one in Admin/connections in the
                                web server)
    :type conn_id_destination: str
    :param sql_query_params: params for query to fetch results from SOURCE
    :type sql_query_params: dict
    :param load_all: if true, will ignore destination's date and will copy everything from source to destination
    :type load_all: bool
    :return: String with table extracted and connections
    """

    table_name_destination = sql_query_params["table_name_destination"]

    sql_query = query.create_query(conn_id_source, conn_id_destination, sql_query_params, load_all)

    try:
        # creates the SQL query for getting the source rows as an iterator of pandas DF
        iter_df_source = get_pandas_iterator(conn_id_source, sql_query)

        # give as many rows in the df as in globvars.max_num_rows_to_df to avoid OutOfMemoryError
        for df in iter_df_source:
            load_rows_to_destination(df, conn_id_destination, table_name_destination, load_all)

    except:
        raise DBNotLoadedException(
            "Could not load table into '{}':\n{}".format(table_name_destination, traceback.format_exc()))

    return "Finished EL from '{}' to '{} - {}'".format(conn_id_source, conn_id_destination, table_name_destination)


def dummy_operation(conn_id_extract, conn_id_load, table_name, **kwargs):
    """
    Only prints into the logs
    :param conn_id_extract: Connection id from source
    :param conn_id_load: Connection id from destination
    :param table_name: Table name
    :param kwargs: kwargs from airflow
    :return: None
    """
    logging.info(
        "Executting dummy_operation for conn_id_extract: '{conn_id_load}', conn_id_load: "
        "'{conn_id_load}', table_name: '{table_name}'".format(
            conn_id_extract=conn_id_extract, conn_id_load=conn_id_load, table_name=table_name))
