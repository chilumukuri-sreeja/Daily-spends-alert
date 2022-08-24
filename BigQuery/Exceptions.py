import logging
from pathlib import Path


class InvalidCredentials(Exception):
    '''Invalid Credentials'''

    def __init__(self) -> None:
        self.messsage = f'''[-] Invalid Credentials'''
        logging.error(self.messsage)
        super().__init__(self.messsage)

class QueryFailed(Exception):
    '''Query Failed to Execute'''

    def __init__(self, query: str, error: Exception) -> None:
        self.message = f'''
            [-] Failed to execute query
            [-] Error : {error}
            [-] Failed Query : {query}
        '''
        logging.error(self.message)
        super().__init__(self.message)

class InvalidFilePath(Exception):
    '''Invalid File Path'''

    def __init__(self, file_path: Path) -> None:
        self.message = f'''[-] Invalid File Path: {file_path}'''
        logging.error(self.message)
        super().__init__(self.message)

class LoadJobFailed(Exception):
    '''BigQuery Load Table Job Failed'''

    def __init__(self, table_name: str, error: Exception): 
        self.message = f'''
            [-] Table Load Job Failed
            [-] Error : {error}
            [-] Destination Table : {table_name}
        '''
        logging.error(self.message)
        super().__init__(self.message)