
# max number of rows allowed inside a pandas df. Pandas will process the query and only extract this many rows at a time
# in a iterator. Successive calls in this iterator return a pandas df with this many rows until finished
max_num_rows_to_df = 25000
