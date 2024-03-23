import os
from bigquery import BigQueryDataLoader, BigQueryExtractor, get_bigquery_client
from soda.scan_operations import run_soda_scan
from duckdb_operations import duck_read_csv_to_dataframe, duck_transform_data
from loguru import logger
import sys

# Configuração do log
logger.remove()
logger.add(sys.stdout, 
           colorize=True, 
           format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | {level} | {message}", 
           level="INFO"
        )


def main():
    # Variáveis de ambiente
    project_id = os.getenv("PROJECT_ID")
    dataset_id = os.getenv("DATASET_ID")
    location = os.getenv("LOCATION")
    
    # Cliente BigQuery
    client_bq = get_bigquery_client(project_id)
    
    # Caminho do arquivo de origem dos dados
    path_file_data_source = "input_data/orders.csv"
    
    # Configuração de carga
    write_disposition = "WRITE_TRUNCATE"
    
    # Fonte de dados SODA
    soda_data_source = "bigquery_soda"

    # Variáveis de ingestão
    table_raw = "raw_orders"
    scan_name_raw = table_raw
    checks_subpath_raw = "raw"
    
    # Variáveis de transformação
    table_trusted = "trusted_orders"
    scan_name_trusted = table_trusted
    checks_subpath_trusted = "trusted"
    
    # Objetos
    bq_extractor = BigQueryExtractor(client_bq)
    bq_loader = BigQueryDataLoader(project_id, location, client_bq)
    
    # Carrega os dados brutos do arquivo CSV para o BigQuery
    logger.info("Starting order pipeline!")
    logger.info(f"Reading source CSV {path_file_data_source}...")
    df_duck_raw = duck_read_csv_to_dataframe(path_file_data_source)
    bq_loader.load_dataframe(
                dataframe=df_duck_raw,
                dataset_id=dataset_id,
                table_name=table_raw,
                write_disposition=write_disposition,
                location=location
    )

    # Executa verificação SODA após a ingestão
    logger.info(f"Running SODA Scan {table_raw}...")
    run_soda_scan(
        data_source=soda_data_source,
        scan_name=scan_name_raw,
        checks_subpath=checks_subpath_raw
    )
    logger.success(f"SODA Scan {table_raw} completed!")

    # Extrai os dados brutos do BigQuery para um DataFrame
    logger.info("Extracting raw data from BigQuery...")
    df_bigquery_raw = bq_extractor.extract_data_from_bigquery_query(
            query=f"SELECT * FROM {project_id}.{dataset_id}.{table_raw}"
    )
    logger.success("Raw data extracted successfully!")
    
    # Transforma os dados brutos
    logger.info("Transforming the raw data...")
    df_duck_transform = duck_transform_data(df_bigquery_raw)
    logger.success("Raw data successfully transformed!")
    
    # Carrega os dados transformados para o BigQuery
    bq_loader.load_dataframe(
                dataframe=df_duck_transform,
                dataset_id=dataset_id,
                table_name=table_trusted,
                write_disposition=write_disposition,
                location=location
    )
    
    # Executa verificação SODA após a transformação
    logger.info(f"Running SODA Scan {table_trusted}...")
    run_soda_scan(
        data_source=soda_data_source, 
        scan_name=scan_name_trusted, 
        checks_subpath=checks_subpath_trusted
    )
    logger.success(f"SODA Scan {table_trusted} completed!")

if __name__ == "__main__":
    main()
