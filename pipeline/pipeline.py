import duckdb
import os
from bigquery_load_data import BigQueryLoadData
from soda.scan_operations import run_soda_scan

# BigQuery variables
project_id = os.getenv("PROJECT_ID")
dataset_id = os.getenv("DATASET_ID")
location = os.getenv("LOCATION")
table_id = "orders"
write_disposition="WRITE_TRUNCATE"

# Soda variables
data_source = "bigquery_soda"
checks_subpath = "ingest"
scan_name = "orders"

path_file = "data/orders.csv"

df_duck = duckdb.read_csv(path_file).df()

load_bq = BigQueryLoadData(
            data_source=df_duck,
            project_id=project_id,
            dataset_id=dataset_id,
            table_name=table_id,
            write_disposition=write_disposition,
            location = location
        )

run_soda_scan(data_source=data_source, scan_name=scan_name, checks_subpath=checks_subpath)


