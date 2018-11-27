from airflow.hooks.mysql_hook import MySqlHook
import traceback
import pandas as pd
import logging
import db.hooks as hooks
import config.globvars as globvars


class DBNotLoadedException(Exception):
    """
    Exception thrown when there is a problem inside the EL process
    """
    pass


def extract_load_table(conn_id_extract, conn_id_load, table_name, load_all=False):
    """
    Extract all rows that match "updated_at > last_destination_updated_at" or if the destination table is empty
    (updated_at = None) then all table is copied from source and pasted in destination.
    :param conn_id_extract: connection id from source (has to match with one in Admin/connections in the web server)
    :type conn_id_extract: str
    :param conn_id_load: connection id from destination (has to match with one in Admin/connections in the web server)
    :type conn_id_load: str
    :param table_name: table name to extract from and load to. Has to exist in both schemas
    :type table_name: str
    :param load_all: if true, will ignore destination's date and will copy everything from source to destination
    :type load_all: bool
    :return: String with table extracted and connections
    """
    try:
        mysql_hook_extract = hooks.MyMysqlHook(conn_id_extract)

        logging.info("Working with table '{}' in connection '{}'...".format(table_name, conn_id_extract))

        # if we want to load all no need to know the max updated_at
        if not load_all:
            max_timestamp_destination = get_max_updated_at(conn_id_load, table_name)
            logging.info("Max destination timestamp: '{}'".format(max_timestamp_destination))

            if max_timestamp_destination is None:  # means, nothing on the table or no updated_at filled
                # Select everything to copy
                logging.warning("WATCH OUT: max_timestamp_destination = None -> copying all table")
                sql_query = "SELECT * FROM {}".format(table_name)
            else:
                sql_query = "SELECT * FROM {} WHERE updated_at > '{}'".format(table_name, max_timestamp_destination)
        else:
            # just select all every row
            sql_query = "SELECT * FROM {}".format(table_name)

        logging.info("Getting iterator of pandas df with chunks of '{}' rows".format(globvars.max_num_rows_to_df))
        iter_df = mysql_hook_extract.get_pandas_df_iterable(sql_query, globvars.max_num_rows_to_df)
        logging.info("Done getting iterator of df!")

        # start looping through the iterator to get all the rows. As many rows in the df as
        # in globvars.max_num_rows_to_df. This is done in order to keep the system from storing huge tables in memory
        # and throw OutOfMemoryError
        for df in iter_df:
            # extract every table as a data frame and convert it to object type -> NaT values then are treated as null
            # objects and can be converted to None
            df = df.astype(object)

            # convert nulls to None (needed in MySQL upload)
            logging.debug("Convert NaN, NaT -> None")
            df = df.where(pd.notnull(df), None)

            logging.info("Column fields from source: {}".format(list(df.keys())))
            logging.info("Row Count from chunk source: '{}'".format(df.shape[0]))

            if not load_all:  # just load the part that has updated_at > last_destination_updated_at
                mysql_hook_load = hooks.MyMysqlHook(conn_id_load)

                # replace should be false, but cannot be sure that we are not repeating values
                mysql_hook_load.insert_update_on_duplicate_rows(table_name, rows=df.values.tolist(),
                                                                columns=(list(df.keys())),
                                                                commit_every=1000)

            else:  # load everything replacing any value if the same PK is found
                mysql_hook_load = MySqlHook(conn_id_load)
                mysql_hook_load.insert_rows(table_name, df.values.tolist(), target_fields=(list(df.keys())),
                                            commit_every=1000,
                                            replace=True)

    except:
        raise DBNotLoadedException(
            "The table {} from extract connection {} to load connection {} was not loaded. Stack error:\n{}".format(
                table_name, conn_id_extract, conn_id_load, traceback.format_exc()))

    return "Finished EL for table '{}' from '{}' to '{}'".format(table_name, conn_id_extract, conn_id_load)


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
