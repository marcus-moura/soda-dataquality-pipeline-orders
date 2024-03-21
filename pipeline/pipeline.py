import os
from bigquery import BigQueryLoadData, bigquery_run_query_to_dataframe, get_bigquery_client
from soda.scan_operations import run_soda_scan
import pandas as pd
from duckdb_operations import duck_load_csv_to_dataframe, duck_transform_data


def main():
    project_id = os.getenv("PROJECT_ID")
    dataset_id = os.getenv("DATASET_ID")
    location = os.getenv("LOCATION")
    client_bq = get_bigquery_client(project_id)
    path_file = "data/orders.csv"
    write_disposition="WRITE_TRUNCATE"
    data_source_soda = "bigquery_soda"

    # Variables ingest
    table_ingest = "raw_orders"
    scan_name_ingest = "orders"
    checks_subpath_ingest = "ingest"
    
    # Variables transform
    table_transform = "trusted_orders"
    scan_name_transform = "orders"
    checks_subpath_transform = "transform"
    
    df_duck_raw = duck_load_csv_to_dataframe(path_file)
    
    BigQueryLoadData(
                data_source=df_duck_raw,
                project_id=project_id,
                dataset_id=dataset_id,
                table_name=table_ingest,
                write_disposition=write_disposition,
                location = location,
                client=client_bq
    )

    run_soda_scan(
        data_source=data_source_soda, 
        scan_name=scan_name_ingest, 
        checks_subpath=checks_subpath_ingest
    )

    df_bigquery_raw = bigquery_run_query_to_dataframe(
            query=f"SELECT * FROM {project_id}.{dataset_id}.{location}", 
            bigquery_client=client_bq
    )
    
    df_duck_transform = duck_transform_data(df_bigquery_raw)
    
    BigQueryLoadData(
                data_source=df_duck_transform,
                project_id=project_id,
                dataset_id=dataset_id,
                table_name=table_transform,
                write_disposition=write_disposition,
                location = location,
                client=client_bq
    )
    
    run_soda_scan(
        data_source=data_source_soda, 
        scan_name=scan_name_transform, 
        checks_subpath=checks_subpath_transform
    )
    