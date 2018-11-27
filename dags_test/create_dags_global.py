import utils.utils as dag_utils
import config.config as config

num_dags = 5

d_config = {
    "conn_id_extract": "test_source",
    "conn_id_load": "test_destination",
    "tables": config.tables_test
}

for i in range(num_dags):
    dag_name = "dag_automatic_{}".format(i)
    globals()[dag_name] = dag_utils.dag_el("dag_automatic_{}".format(i), d_config, config.default_args)
