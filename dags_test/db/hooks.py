from airflow.hooks.mysql_hook import MySqlHook

from MySQLdb import cursors
from MySQLdb.compat import unicode
from _mysql_exceptions import ProgrammingError

from contextlib import closing
import logging
import sys

# get if Python version is 2.X. To maintain compatibility
PY2 = sys.version_info[0] == 2


class CustomCursor(cursors.Cursor):
    """
        Subclassed Cursor to have a little bit more vision of Queries being done. Input logs after and before formatting
        the query to see the actual query and the formatting
    """

    def __init__(self, connection):
        cursors.Cursor.__init__(self, connection)

    def execute(self, query, args=None, log=False):
        """
        Execute a query.

        query -- string, query to execute on server
        args -- optional sequence or mapping, parameters to use with query.
        log  -- optional bool, if True will log before and after formatting the query

        Note: If args is a sequence, then %s must be used as the
        parameter placeholder in the query. If a mapping is used,
        %(key)s must be used as the placeholder.

        Returns integer represents rows affected, if any
        """
        while self.nextset():
            pass
        db = self._get_db()

        # NOTE:
        # Python 2: query should be bytes when executing %.
        # All unicode in args should be encoded to bytes on Python 2.
        # Python 3: query should be str (unicode) when executing %.
        # All bytes in args should be decoded with ascii and surrogateescape on Python 3.
        # db.literal(obj) always returns str.

        if PY2 and isinstance(query, unicode):
            query = query.encode(db.encoding)

        if args is not None:
            if isinstance(args, dict):
                args = dict((key, db.literal(item)) for key, item in args.items())
            else:
                args = tuple(map(db.literal, args))
            if not PY2 and isinstance(query, (bytes, bytearray)):
                query = query.decode(db.encoding)
            try:
                if log:
                    logging.info("Query '{}' - Args '{}'".format(query, args))
                    query = query % args
                    logging.info("Query after formatting: '{}'".format(query))
                else:
                    query = query % args

            except TypeError as m:
                self.errorhandler(self, ProgrammingError, str(m))

        if isinstance(query, unicode):
            query = query.encode(db.encoding, 'surrogateescape')

        res = None
        try:
            res = self._query(query)
        except Exception:
            exc, value = sys.exc_info()[:2]
            self.errorhandler(self, exc, value)
        self._executed = query
        if not self._defer_warnings:
            self._warning_check()
        return res


class MyMysqlHook(MySqlHook):
    """
    Subclassed MySqlHook from airflow so that insert_rows could be converted to insert_update_on_duplicate_rows.
    Also adds the method get_pandas_df_iterable which was not available in MySQLHook
    """

    def __init__(self, *args, **kwargs):
        MySqlHook.__init__(self, *args, **kwargs)

    def insert_update_on_duplicate_rows(self, table, rows, columns, commit_every=1000):
        """
        A generic way to insert a set of tuples into a table,
        a new transaction is created every commit_every rows

        :param table: Name of the target table
        :type table: str
        :param rows: The rows to insert into the table
        :type rows: iterable of tuples
        :param columns: The names of the columns to fill in the table
        :type columns: iterable of strings
        :param commit_every: The maximum number of rows to insert in one
            transaction. Set to 0 to insert all rows in one transaction.
        :type commit_every: int
        """

        i = 0

        with closing(self.get_conn()) as conn:
            if self.supports_autocommit:
                self.set_autocommit(conn, False)

            conn.commit()

            with closing(conn.cursor(CustomCursor)) as cur:
                for i, row in enumerate(rows, 1):
                    if len(row) != len(columns):
                        raise Exception(
                            "Length Columns and row does not match! Columns: '{}' - Row: '{}'".format(columns, row))

                    zip_list = zip(columns, row)
                    lst = []
                    s_map = ""
                    for s_col, cell in zip_list:  # (s_column_name, row value)
                        serialized_cell = self._serialize_cell(cell, conn)
                        lst.append(serialized_cell)

                        # adds a string with `{s_col}`=value, at the end of the string query
                        s_map += "`{}`=%s, ".format(s_col)

                    s_map = s_map[:-2]
                    values = tuple(lst)
                    placeholders = ["%s", ] * len(values)

                    sql = "INSERT INTO {0} {1} VALUES ({2}) ON DUPLICATE KEY UPDATE {3}".format(
                        table,
                        str(tuple(columns)).replace('"', "'").replace("'", "`"),
                        ",".join(placeholders),
                        s_map)

                    if i == 1:
                        cur.execute(sql, values * 2, log=True)
                    else:
                        cur.execute(sql, values * 2)

                    if commit_every and i % commit_every == 0:
                        conn.commit()
                        self.log.info(
                            "Loaded {i} into {table} rows so far".format(i=i, table=table)
                        )

            conn.commit()
        self.log.info(
            "Done loading. Loaded a total of {i} rows".format(i=i))

    def get_pandas_df_iterable(self, sql, chunksize, parameters=None):
        """
        Returns an iterable of pandas DF with size chunksize

        :param chunksize: Number of rows to fit in one chunk. If there is more rows than chunksize, pandas will return
                            an iterator with rows=chunksize until no more rows are selected in the query
        :type chunksize: int
        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        if sys.version_info[0] < 3:
            sql = sql.encode('utf-8')
        import pandas.io.sql as psql

        with closing(self.get_conn()) as conn:
            return psql.read_sql(sql, con=conn, params=parameters, chunksize=chunksize)


