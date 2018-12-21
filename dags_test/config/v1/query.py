import queries.abcquery as abcquery


class QueryConfig(abcquery.QueryBase):

    def __init__(self):
        abcquery.QueryBase.__init__(self)

    @staticmethod
    def get_base_query(table_name, s_columns):
        sql_query = """
        SELECT {s_columns}
        FROM {table_name} t
        """.format(table_name=table_name, s_columns=s_columns)
        return sql_query

    @staticmethod
    def get_query1(table_name, s_columns):
        pass

    @staticmethod
    def get_query2(table_name, s_columns):
        pass

    @staticmethod
    def get_query3(table_name, s_columns):
        pass

    @staticmethod
    def get_query4(table_name, s_columns):
        pass


