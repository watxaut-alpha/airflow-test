
tables_test = [
    "customers",
    "employees",
    "offices",
    "orderdetails",
    "orders",
    "payments",
    "productlines",
    {
        "table_name_source": "products",
        "table_name_destination": "products_extend_with_customers",
        "operation_type": "query2",
        "query_type": "customers",
        "l_columns_exclude": ["col1", "col2"]
    }
]

