import logging
from pathlib import Path

from BigQuery.Exceptions import *

from google.cloud import bigquery
from config import CONFIGURATION
from google.cloud.bigquery import LoadJobConfig
from google.cloud.bigquery import LoadJob, QueryJob
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery import Table
from google.auth.credentials import Credentials

from typing import List, TypedDict
from Authentication.Authentication import Authentication


# Schema Object
# - Compliant with schema set in SDFConfigs
class SchemaObject(TypedDict):
    name: str
    sqltype: str

class BigQuery:
    
    _credentials: Credentials
    client: bigquery.Client

    def __init__(self, credentials: Credentials = None):

        if credentials == None:
            scopes = CONFIGURATION.get("SCOPES").get("BIGQUERY")
            auth_handler = Authentication()
            credentials = auth_handler.getServiceAccountCredentials(scopes = scopes)

        self._credentials = credentials
        self.client = bigquery.Client(credentials=self._credentials)

    def tableExists(self, table_name) -> bool:
        '''Check if Table Exists
        Arguments:
            - table_name (str) : Fully qualified table name in format `<project-id>.<dataset-id>.<table_name>`
        '''

        try:
            table: Table = self.client.get_table(table_name)
            return True
        except Exception as error:
            return False

    def getTableSchema(self, table_name) -> List[SchemaField]:
        '''Check if Table Exists
        Arguments:
            - table_name (str) : Fully qualified table name in format `<project-id>.<dataset-id>.<table_name>`
        '''

        try:
            table: Table = self.client.get_table(table_name)
            return table.schema
        except Exception as error:
            return False

    def createTable(self, table_name: str, schema: List[SchemaField]):
        '''Create a BigQuery Table with a certain schema
        Arguments:
            - table_name (str) : Fully qualified table name in format `<project-id>.<dataset-id>.<table_name>`
            - schema (List[google.cloud.bigquery.SchemaField]) : Schema Field
        '''

        table: Table = Table(table_name, schema=schema)
        table = self.client.create_table(table)


    def runQuery(self, query: str) :
        '''Run a Query on BigQuery
        Arguments: 
            - query (str) : Query to be executed
        '''

        logging.debug(f"[+] Running Query : {query} ...")
        try:
            job: QueryJob = self.client.query(query)
            result = job.result()
        except Exception as error:
            raise QueryFailed(query, error)
        logging.debug(f"[+] Query Run Successfully !!!")
        
        results = []
        for row in result:
            results.append(dict(row))

        logging.debug(f"[+] Sending Query Result")
        return results

    def loadTableFromJSON(self, 
        table_name: str, 
        json_data: List[dict], 
        schema: List[SchemaField], 
        write_disposition: str = "WRITE_TRUNCATE") -> None:
        
        '''Load Data to BigQuery from JSON
        Arguments: 
            - table_name (str) : Destination Table
            - json_data: List[dict] : JSON Data to be uploaded
            - schema (List[SchemaField]) : Schema of the table to be loaded into
        '''

        if len(json_data) == 0 : return 
        if not self.tableExists(table_name):
            self.client.create_table(table_name, schema)

        logging.debug(f"[+] Setting BigQuery LoadJob Configuration")
        job_config: LoadJobConfig = LoadJobConfig(
            schema = schema,
            write_disposition = write_disposition
        )
        
        logging.debug(f"[+] Writing Data to BigQuery Table: {table_name} ...")
        job = None
        try:
            job: LoadJob = self.client.load_table_from_json(
                json_rows = json_data,
                destination = table_name,
                job_config = job_config 
            )
            job.result()
        except Exception as error:
            logging.error(f"[+] Load Job on {table_name} failed. Error : {error}")
            raise LoadJobFailed(table_name, error)
        logging.debug(f"[+] Data Written to BigQuery table: {table_name} !!!")
        

    def loadDataFromFile(self, table_name: str, file_path: Path, schema: List[SchemaField], write_disposition: str = "WRITE_TRUNCATE"):
        '''Load Data To BigQuery from a File
        Arguments:
            - table_name (str) : Destination Table Name
            - file_path (Path) : File Path of the file to be loaded
            - schema (List[SchemaObject]) : Schema of the table to be loaded into 
        '''

        if not file_path.exists(): raise InvalidFilePath(file_path)
        if not self.tableExists(table_name):
            self.client.create_table(table_name, schema)

        logging.debug(f"[+] Setting BigQuery LoadJob Configuration")
        job_config: LoadJobConfig = LoadJobConfig(
            schema = schema,
            write_disposition = write_disposition,
            source_format = bigquery.SourceFormat.CSV,
            skip_leading_rows = 1,
        )
        
        logging.debug(f"[+] Writing File to BigQuery ...")
        job = None
        try:
            with open(file_path, "rb") as data_file:
                job: LoadJob = self.client.load_table_from_file(
                    file_obj = data_file,
                    destination = table_name,
                    job_config = job_config 
                )
            job.result()
        except Exception as error:
            raise LoadJobFailed(table_name, error)
        logging.debug(f"[+] File Written to BigQuery !!!")
