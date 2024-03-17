import os
from google.cloud import bigquery
from google.oauth2 import service_account
from google.auth.exceptions import DefaultCredentialsError
from loguru import logger
import pandas as pd

def get_bigquery_client(project_id: str) -> bigquery.Client:
    """ Obtém o client do BigQuery """
    try:
        service_account_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

        if service_account_path:
            credentials = service_account.Credentials.from_service_account_file(
                service_account_path
            )
            bigquery_client = bigquery.Client(
                project=project_id, credentials=credentials
            )
            return bigquery_client

        raise EnvironmentError(
            "No valid credentials found for BigQuery authentication."
        )

    except DefaultCredentialsError as creds_error:
        raise creds_error


def bigquery_run_query_to_dataframe(
    query: str, bigquery_client: bigquery.Client
) -> pd.DataFrame:
    """Executa uma query no BigQuery e retorna um DataFrame"""
    try:
        dataframe = bigquery_client.query(query).to_dataframe()
        return dataframe

    except Exception as e:
        logger.exception(f"Error running query: {e}")
        raise
    
class BigQueryLoadDataError(Exception):
    pass

class BigQueryLoadData:
    def __init__(
        self,
        data_source: dict | pd.DataFrame | str,
        project_id: str,
        dataset_id: str,
        table_name: str,
        schema_fields: list = None,
        description: str = None,
        labels: dict = None,
        source_format: str = None,
        create_disposition: str ='CREATE_IF_NEEDED',
        write_disposition: str = 'WRITE_TRUNCATE',
        autodetect: bool = True,
        skip_leading_rows: int = 0,
        time_partitioning: str = None,
        partition_field: str = None,
        location: str = 'southamerica-east1',
        schema_relax: bool = False
    ):
        
        self.data_source = data_source
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_name = table_name
        self.schema_fields = schema_fields
        self.description = description
        self.labels = labels
        self.source_format = source_format
        self.create_disposition = create_disposition
        self.write_disposition = write_disposition
        self.autodetect = autodetect
        self.skip_leading_rows = skip_leading_rows
        self.time_partitioning = time_partitioning
        self.partition_field = partition_field
        self.location = location
        
        self.schema_relax = schema_relax
        self.client = bigquery.Client()

        if time_partitioning:
            if time_partitioning.lower() == 'day':
                self.time_partitioning = bigquery.TimePartitioningType.DAY
            elif time_partitioning.lower() == 'hour':
                self.time_partitioning = bigquery.TimePartitioningType.HOUR
            elif time_partitioning.lower() == 'month':
                self.time_partitioning = bigquery.TimePartitioningType.MONTH
            else:
                self.time_partitioning = bigquery.TimePartitioningType.YEAR
        
        if source_format:
            if source_format.lower() == 'csv':
                self.source_format = bigquery.SourceFormat.CSV,
            elif source_format.lower() == 'parquet':
                self.source_format = bigquery.SourceFormat.PARQUET,
            else:
                self.source_format = bigquery.SourceFormat.AVRO

            
        self.execute()
        
    def execute(self):

        destination = f"{self.project_id}.{self.dataset_id}.{self.table_name}"
        logger.info(f"Starting Load data into the table {self.table_name} in BigQuery!")
        
        try:
            if isinstance(self.data_source, pd.DataFrame):
                self.load_dataframe_to_bigquery(destination)
            elif isinstance(self.data_source, dict):
                self.load_json_to_bigquery(destination)
            else:
                self.load_file_to_bigquery(destination)
                
            if self.labels:
                self.add_labels(destination)
        
        except Exception as e:
            raise BigQueryLoadDataError(f"Failed to insert data into BigQuery: {str(e)}")

    def add_labels(self, abs_path_table: str):
        table = self.client.get_table(abs_path_table)
        existing_labels = table.labels
        if existing_labels != self.labels:
            table.labels = self.labels
            table = self.client.update_table(table, ["labels"])
            
            logger.info(f"Added labels to {self.table_name}.")
        
    def load_json_to_bigquery(self, destination):
        
        job_config = bigquery.LoadJobConfig(
            schema=self.schema_fields,
            destination_table_description=self.description,
            create_disposition=self.create_disposition,
            write_disposition=self.write_disposition,
            autodetect=self.autodetect,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            time_partitioning=bigquery.TimePartitioning(type_=self.time_partitioning, field=self.partition_field) if self.time_partitioning else None
        )

        job = self.client.load_table_from_json(json_rows=self.data_source,
                                          destination=destination,
                                          location=self.location,
                                          job_config=job_config)
        try:
            logger.info(f"Inserting data into the {self.table_name} in BigQuery!")
            job.result()
            logger.success(f"Successfully entered data")
        except Exception as e:
            raise BigQueryLoadDataError(f"Failed to insert data into BigQuery: {str(e)}")
            

    def load_dataframe_to_bigquery(self, destination):
        job_config = bigquery.LoadJobConfig(
            schema=self.schema_fields,
            destination_table_description=self.description,
            autodetect=self.autodetect,
            create_disposition=self.create_disposition,
            schema_update_options=bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION if self.schema_relax else None,
            write_disposition=self.write_disposition,
            time_partitioning=bigquery.TimePartitioning(type_=self.time_partitioning, field=self.partition_field) if self.time_partitioning else None
        )

        job = self.client.load_table_from_dataframe(dataframe=self.data_source,
                                          destination=destination,
                                          location=self.location,
                                          job_config=job_config)
        try:
            logger.info(f"Inserting data into the {self.table_name} in BigQuery!")
            job.result()
            logger.success(f"Successfully entered data")
        except Exception as e:
            raise BigQueryLoadDataError(f"Failed to insert data into BigQuery: {str(e)}")
    
    def load_file_to_bigquery(self, destination):
        job_config = bigquery.LoadJobConfig(
            schema=self.schema_fields,
            destination_table_description=self.description,
            autodetect=self.autodetect,
            create_disposition=self.create_disposition,
            write_disposition=self.write_disposition,
            source_format=self.source_format,
            time_partitioning=bigquery.TimePartitioning(type_=self.time_partitioning, field=self.partition_field) if self.time_partitioning else None
        )

        with open(self.data_source, "rb") as source_file:
            job = self.client.load_table_from_file(file_obj=source_file,
                                            destination=destination,
                                            location=self.location,
                                            job_config=job_config)
            try:
                logger.info(f"Inserting data into the {self.table_name} in BigQuery!")
                job.result()
                logger.success(f"Successfully entered data")
            except Exception as e:
                raise BigQueryLoadDataError(f"Failed to insert data into BigQuery: {str(e)}")
