import logging

from google.api_core.exceptions import NotFound 
from google.cloud.pubsub_v1 import PublisherClient
from google.auth.credentials import Credentials

from config import CONFIGURATION
from Authentication.Authentication import Authentication        


class PubSub:
    '''Wrapper class to interact with Google Pubsub Library in a credential regulated way'''

    _project_id: str
    _crednentials: Credentials
    _publisher_client: PublisherClient
    _topic_path: str

    def __init__(self, topic_path:str = CONFIGURATION.get("TOPIC_PATH") ):
        
        scopes = CONFIGURATION.get("SCOPES").get("PUBSUB")
        auth_handler: Authentication = Authentication()

        self._project_id = CONFIGURATION.get("PROJECT_ID")
        self._crednentials = auth_handler.getServiceAccountCredentials(scopes=scopes)
        self._publisher_client = PublisherClient(credentials=self._crednentials)

        self._topic_path = topic_path

    def publishMessage(self, message: str = "", attribute: dict = {}):

        try:
            logging.debug(f"[+] Fetching Topic : {self._topic_path}")
        
            data = str(message).encode("utf-8")
            
            logging.debug(f"[+] Publishing Message: {message} to Topic: {self._topic_path}")
            future = self._publisher_client.publish(self._topic_path, data, **attribute )
            print(f"Published message ID: {future.result()}")

        except NotFound:
            raise Exception(f"Topic Not Found: {self._topic_path}")