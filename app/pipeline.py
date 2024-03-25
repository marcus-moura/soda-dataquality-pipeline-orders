from app.bigquery import BigQueryDataLoader, BigQueryExtractor, get_bigquery_client
from app.soda.scan_operations import run_soda_scan
from app.duckdb_operations import duck_read_csv_to_dataframe, duck_transform_data
from app import utils
from loguru import logger
import sys
import typer

app = typer.Typer()

# Log Config
logger.remove()
logger.add(sys.stdout, colorize=True, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | {level} | {message}")
logger.level("INFO")
    
@app.command()
def run(project_id: str = typer.Option(..., "--project_id"),
        location: str = typer.Option(..., "--location"),
        dataset_id: str = typer.Option(..., "--dataset_id"),
        file_source: str = typer.Option(..., "--file_source"),
        table_raw: str = typer.Option(..., "--table_raw"),
        table_trusted: str = typer.Option(..., "--table_trusted"),
        write_disposition: str = typer.Option("WRITE_TRUNCATE", "--write_disposition"),
        soda_data_source: str = typer.Option("bigquery_soda", "--soda_data_source"),
        checks_subpath_raw: str = typer.Option("raw", "--checks_subpath_raw"),
        checks_subpath_trusted: str = typer.Option("trusted", "--checks_subpath_trusted")
    ):

    # BigQuery Client
    client_bq = get_bigquery_client(project_id)
    # Objects
    bq_extractor = BigQueryExtractor(client_bq)
    bq_loader = BigQueryDataLoader(project_id, location, client_bq)
    
    path_file_data_source = utils.get_file_path(file_source)
    
    # Load raw data from CSV file into BigQuery
    logger.info("Starting order pipeline!")
    logger.info(f"Reading source CSV {path_file_data_source}...")
    df_duck_raw = duck_read_csv_to_dataframe(path_file_data_source)
    
    bq_loader.load_dataframe(dataframe=df_duck_raw,
                             dataset_id=dataset_id,
                             table_name=table_raw,
                             write_disposition=write_disposition, 
                             location=location
    )

    # Run SODA check after ingestion
    logger.info(f"Running SODA Scan {table_raw}...")
    run_soda_scan(data_source=soda_data_source, scan_name=table_raw, checks_subpath=checks_subpath_raw)
    logger.success(f"SODA Scan {table_raw} completed!")

    # Extract raw data from BigQuery into a DataFrame
    logger.info("Extracting raw data from BigQuery...")
    df_bigquery_raw = bq_extractor.extract_data_from_bigquery_query(
            query=f"SELECT * FROM {project_id}.{dataset_id}.{table_raw}"
    )
    logger.success("Raw data extracted successfully!")
    
    # Transform the raw data
    logger.info("Transforming the raw data...")
    df_duck_transform = duck_transform_data(df_bigquery_raw)
    logger.success("Raw data successfully transformed!")
    
    # Load the transformed data into BigQuery
    bq_loader.load_dataframe(dataframe=df_duck_transform,
                             dataset_id=dataset_id,
                             table_name=table_trusted,
                             write_disposition=write_disposition,
                             location=location
    )
    
    # Run SODA check after transformation
    logger.info(f"Running SODA Scan {table_trusted}...")
    run_soda_scan(data_source=soda_data_source, scan_name=table_trusted, checks_subpath=checks_subpath_trusted)
    logger.success(f"SODA Scan {table_trusted} completed!")

@app.callback()
def callback():
    """
    Execute a data quality pipeline with SODA.
    """

if __name__ == "__main__":
    app()
