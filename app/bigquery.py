import os
from google.cloud import bigquery
from google.oauth2 import service_account
from google.auth.exceptions import DefaultCredentialsError
from loguru import logger
import pandas as pd

def get_bigquery_client(project_id: str) -> bigquery.Client:
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

class BigQueryExtractor:
    def __init__(self, client: bigquery.Client) -> None:
        self.client = client
        
    def extract_data_from_bigquery_query(self, query: str) -> pd.DataFrame:
        """Extracts data from a BigQuery query and returns a DataFrame."""
        try:
            dataframe = self.client.query(query).to_dataframe()
            return dataframe

        except Exception as e:
            logger.exception(f"Error running query: {e}")
            raise

class BigQueryDataLoaderError(Exception):
    pass

class BigQueryDataLoader:
    """
    Class responsible for loading data into BigQuery.
    
    param: project_id (str): The ID of the Google Cloud project.
    param: location (str): The location of the dataset.
    param: client (bigquery.Client, optional): The BigQuery client. Defaults to bigquery.Client().
    """
    def __init__(self, 
        project_id: str, 
        location: str,
        client: bigquery.Client = bigquery.Client()
        ) -> None:
        self.project_id = project_id
        self.location = location
        self.client = client

    def _create_job_config(self, **kwargs) -> bigquery.LoadJobConfig:
        """
        Creates a job configuration for loading data into BigQuery.

        param: schema_fields (list, optional): The schema fields for the data. Defaults to None.
        param: description (str, optional): The table description. Defaults to None.
        param: create_disposition (str, optional): The table creation disposition. Defaults to 'CREATE_IF_NEEDED'.
        param: write_disposition (str): The write disposition for the job. Defaults to None.
        param: autodetect (bool, optional): Whether to automatically detect schema. Defaults to True.
        param: schema_relax (bool, optional): Whether to allow schema field addition. Defaults to False.
        param: time_partitioning (str, optional): The type of time partitioning. Defaults to None.
        param: partition_field (str, optional): The field to partition the data by. Defaults to None.

        Returns:
            bigquery.LoadJobConfig: The job configuration object.
        """
        time_partitioning = kwargs.get('time_partitioning')
        if time_partitioning:
            time_partitioning = time_partitioning.upper()
        
        job_config = bigquery.LoadJobConfig(
            schema=kwargs.get('schema_fields'),
            destination_table_description=kwargs.get('description'),
            create_disposition=kwargs.get('create_disposition', 'CREATE_IF_NEEDED'),
            write_disposition=kwargs.get('write_disposition'),
            autodetect=kwargs.get('autodetect', True),
            schema_update_options=bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION if kwargs.get('schema_relax') else None,
            time_partitioning=bigquery.TimePartitioning(type_=time_partitioning, field=kwargs.get('partition_field')) if kwargs.get('time_partitioning') else None
        )
        return job_config

    def _add_labels(self, destination: str, labels: str) -> None:
        """
        Adds labels to a BigQuery table.

        param: destination (str): The destination table.
        param: labels (str): The labels to be added.
        """
        table = self.client.get_table(destination)
        existing_labels = table.labels
        if existing_labels != labels:
            table.labels = labels
            table = self.client.update_table(table, ["labels"])
            logger.info(f"Added labels to {table.friendly_name}.")

    def _build_destination_table(self, dataset_id: str, table_name: str) -> str:
        """
        Builds the destination table name in BigQuery.
        
        param: dataset_id (str): The ID of the dataset.
        param:  table_name (str): The name of the table.
        
        Returns:
            str: The destination table name.
        """
        return f"{self.project_id}.{dataset_id}.{table_name}"
    
    def _handler_job_result(self, job, destination: str, table_name: str, labels=None):
        """
        Handles the result of a job execution.

        param: job: The BigQuery job.
        param: destination (str): The destination table name.
        param: table_name (str): The name of the table.
        param: labels (str, optional): The labels to be added. Defaults to None.
        """
        try:
            logger.info(f"Loading data into the table {table_name} in BigQuery!")
            job.result()
            logger.success("Data loaded successfully!")
            if labels:
                self._add_labels(destination, labels)
        except Exception as e:
            raise BigQueryDataLoaderError(f"Failed to insert data into BigQuery: {str(e)}")
        
    def load_json(self, json_rows: dict, dataset_id: str, table_name: str, **kwargs):
        """
        Loads JSON data into BigQuery.

        param: json_rows (dict): The JSON data to be loaded.
        param: dataset_id (str): The ID of the dataset.
        param: table_name (str): The name of the table.
        param: **kwargs: Additional keyword arguments for configuring the job.
        """
        destination = self._build_destination_table(dataset_id, table_name)
        
        job_config = self._create_job_config(**kwargs)
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        
        job = self.client.load_table_from_json(
                                        json_rows=json_rows,
                                        destination=destination,
                                        location=self.location,
                                        job_config=job_config)
        
        self._handler_job_result(job, destination, table_name, kwargs.get('labels'))

    def load_dataframe(self, dataframe: pd.DataFrame, dataset_id: str, table_name: str, **kwargs):
        """
        Loads DataFrame data into BigQuery.

        param: dataframe (pd.DataFrame): The DataFrame data to be loaded.
        param: dataset_id (str): The ID of the dataset.
        param: table_name (str): The name of the table.
        param: **kwargs: Additional keyword arguments for configuring the job.
        """
        destination = self._build_destination_table(dataset_id, table_name)
        
        job_config = self._create_job_config(**kwargs)
        
        job = self.client.load_table_from_dataframe(
                                            dataframe=dataframe, 
                                            destination=destination, 
                                            location=self.location, 
                                            job_config=job_config)
        self._handler_job_result(job, destination, table_name, kwargs.get('labels'))

    def load_file(self, 
                file_source: str, 
                source_format: str, 
                dataset_id: str, 
                table_name: str, 
                field_delimiter: str = None, 
                encoding: str = None,
                skip_rows: int = 0,
                **kwargs
        ):
        """
        Loads data from a file into BigQuery.

        param: file_source (str): The source file path.
        param: source_format (str): The format of the source data.
        param: dataset_id (str): The ID of the dataset.
        param: table_name (str): The name of the table.
        param: field_delimiter (str, optional): The field delimiter. Defaults to None.
        param: encoding (str, optional): The encoding of the file. Defaults to None.
        param: skip_rows (int, optional): The number of rows to skip. Defaults to 0.
        param: **kwargs: Additional keyword arguments for configuring the job.
        """
        destination = self._build_destination_table(dataset_id, table_name)
        
        job_config = self._create_job_config(**kwargs)
        job_config.source_format = getattr(bigquery.SourceFormat, source_format.upper(), None)
        job_config.field_delimiter = field_delimiter
        job_config.encoding = encoding
        job_config.skip_leading_rows = skip_rows
        
        with open(file_source, "rb") as file_obj:
            job = self.client.load_table_from_file(
                                                file_obj=file_obj,
                                                destination=destination,
                                                location=self.location,
                                                job_config=job_config)
            
            self._handler_job_result(job, destination, table_name, kwargs.get('labels'))
        