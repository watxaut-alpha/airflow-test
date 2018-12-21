import utils.utils as dag_utils
import config.v1.table_definitions as tables
import config.config as config

num_dags = 5

d_config = {
    "conn_id_extract": "test_source",
    "conn_id_load": "test_destination",
    "tables": tables.tables_test
}

conn_id_source = d_config["conn_id_extract"]
conn_id_destination = d_config["conn_id_load"]
tables = d_config["tables"]

for i in range(num_dags):
    dag_name = "dag_automatic_{}".format(i)
    globals()[dag_name] = dag_utils.dag_el("dag_automatic_{}".format(i), conn_id_source, conn_id_destination, tables,
                                           config.default_args)
