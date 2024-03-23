import os
from bigquery import BigQueryDataLoader, BigQueryExtractor, get_bigquery_client
from soda.scan_operations import run_soda_scan
from duckdb_operations import duck_read_csv_to_dataframe, duck_transform_data


def main():
    # Environment variables
    project_id = os.getenv("PROJECT_ID")
    dataset_id = os.getenv("DATASET_ID")
    location = os.getenv("LOCATION")
    
    #Client BigQuery
    client_bq = get_bigquery_client(project_id)
    
    # File path data source
    path_file_data_source = "data/orders.csv"
    
     # Load configuration
    write_disposition="WRITE_TRUNCATE"
    
    # SODA data source
    soda_data_source = "bigquery_soda"

    # Ingestion variables
    table_raw= "raw_orders"
    scan_name_raw = table_raw
    checks_subpath_raw = "raw"
    
    # Transformation variables
    table_trusted = "trusted_orders"
    scan_name_trusted = table_trusted
    checks_subpath_trusted = "trusted"
    
    # Objects
    bq_extractor = BigQueryExtractor(client_bq)
    bq_loader = BigQueryDataLoader(project_id, location, client_bq)
    
    # Load raw data from CSV file to BigQuery
    df_duck_raw = duck_read_csv_to_dataframe(path_file_data_source)
    bq_loader.load_dataframe(
                dataframe=df_duck_raw,
                dataset_id=dataset_id,
                table_name=table_raw,
                write_disposition=write_disposition,
                location = location,
                client=client_bq
    )

    # Run SODA check after ingestion
    run_soda_scan(
        data_source=soda_data_source,
        scan_name=scan_name_raw,
        checks_subpath=checks_subpath_raw
    )

    # Extract raw data from BigQuery to a DataFrame
    df_bigquery_raw = bq_extractor.extract_data_from_bigquery_query(
            query=f"SELECT * FROM {project_id}.{dataset_id}.{location}", 
            client=client_bq
    )
    
    # Transform raw data
    df_duck_transform = duck_transform_data(df_bigquery_raw)
    
    # Load transformed data to BigQuery
    bq_loader.load_dataframe(
                dataframe=df_duck_transform,
                dataset_id=dataset_id,
                table_name=table_trusted,
                write_disposition=write_disposition,
                location = location
    )
    
    # Run SODA check after transformation
    run_soda_scan(
        data_source=soda_data_source, 
        scan_name=scan_name_trusted, 
        checks_subpath=checks_subpath_trusted
    )
    

if __name__ == "__main__":
    main()