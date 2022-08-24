import httplib2
import sys
import logging
import os

from google.auth import default
from config import CONFIGURATION
from google.oauth2 import service_account
from google.oauth2.service_account import IDTokenCredentials
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import AuthorizedSession
from google.auth.transport.requests import Request
from oauth2client import GOOGLE_TOKEN_URI 

from typing import List

# Setting Logging Configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)

class Authentication:

    def getClientAccountCredentials(self, scopes: List[str] = []) -> Credentials:
        '''Get Client Account Credentials'''

        CLIENT_ID = os.environ.get("CLIENT_ID")
        CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
        REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN")
        
        logging.debug(f"[+] Fetching Client Account Credentials")
        credentials = Credentials(
            token = None,
            client_id = CLIENT_ID,
            client_secret = CLIENT_SECRET,
            refresh_token = REFRESH_TOKEN,
            token_uri = GOOGLE_TOKEN_URI,
            scopes = scopes
        )

        credentials.refresh(Request())  # refresh the access token (optional)
        return credentials

    def getServiceAccountCredentials(self, scopes: List[str]) -> Credentials:
        '''Get Service Account Credentials'''

        logging.debug(f"[+] Fetching Service Account Credentials")
        credentials, project_id = default(scopes=scopes)
        return credentials

    def generateAuthenticatedRequest(self, target_url: str, method: str = "GET", data: dict = None, post_json: dict = None, headers: dict = None ):
        
        logger.debug(f"[+] Generating Authenticated Request")

        logger.debug(f"[+] Generating ID Token Credentials")

        service_account_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        id_token_credentials: IDTokenCredentials = service_account\
            .IDTokenCredentials\
            .from_service_account_file(service_account_path, target_audience=target_url)
        logger.debug(f"[+] ID Token Credentials generted : {id_token_credentials}")

        logger.debug(f"[+] Creating Authenticated Session")
        authorised_session: AuthorizedSession = AuthorizedSession(id_token_credentials)
        logger.debug(f"[+] Authenticated Session Created")

        logger.debug(f"[+] Sending request")

        if method == "GET":
            response = authorised_session.get(
                url = target_url,
                data = data,
                headers = headers
            )
        
        if method == "POST":
            response = authorised_session.post(
                url = target_url,
                data = data,
                headers = headers,
                json = post_json
            )

        return response