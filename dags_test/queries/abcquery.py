from abc import ABC, abstractmethod


class QueryBase(ABC):

    def __init__(self):

        # MUST define in child classes, as each version may have different columns
        self.param1 = None

        self.d_queries1 = {"q1": self.get_query1, "q2": self.get_query2}
        self.d_queries2 = {"customers": self.get_query3, "orderdetails": self.get_query4}

    def get_query(self, s_type, key):
        if s_type == "base_query":
            return self.get_base_query
        elif s_type == "query1":
            return self.d_queries1[key]
        elif s_type == "query2":
            return self.d_queries2[key]
        else:
            raise Exception("Query type '{}' not found".format(s_type))

    @staticmethod
    @abstractmethod
    def get_base_query(table_name, s_columns):
        pass

    @staticmethod
    @abstractmethod
    def get_query1(table_name, s_columns):
        pass

    @staticmethod
    @abstractmethod
    def get_query2(table_name, s_columns):
        pass

    @staticmethod
    @abstractmethod
    def get_query3(table_name, s_columns):
        pass

    @staticmethod
    @abstractmethod
    def get_query4(table_name, s_columns):
        pass

