import functools
import json
import logging
from pytz import timezone
from datetime import datetime, tzinfo
from typing import List

from config import CONFIGURATION
from BigQuery.BigQuery import BigQuery
from Utility.Utility import getUniqueId, getAdvertiser

from google.cloud.bigquery import SchemaField


class Alert:
    '''Class to regulate all activities in alert-lifecycle'''

    advertiser: str
    advertiser_id: int
    alert_hash: str
    alert_type: str
    alert_data_link: str
    entity_id: int
    entity_type: str
    alert_type: str

    escalation_configuration: dict
    alert_id: str
    alert_data: dict
    alert_message: str
    default_receiver: str
    alert_subject: str
    alert_subtext: str
    alert_header: str
    alert_footer: str
    alert_metadata_table: str



    def __init__(self,
        advertiser_id: int = 0,
        alert_hash: str = "",
        alert_data_link: str = "",
        entity_id: int = 0,
        entity_type: str = CONFIGURATION.get("ALERT_ENTITY"),
        alert_type: str = CONFIGURATION.get("ALERT_TYPE"),
        escalation_configuration: dict = CONFIGURATION.get("ESCALATION_LEVELS"),
        default_receiver: str = CONFIGURATION.get("ALERT_DEFAULT_RECEIVER"),
        alert_message: str = CONFIGURATION.get("ALERT_MESSAGE"),
        alert_subject: str = CONFIGURATION.get("ALERT_SUBJECT"),
        alert_subtext: str = CONFIGURATION.get("ALERT_SUBTEXT"),
        alert_header: str = CONFIGURATION.get("ALERT_HEADER"),
        alert_footer: str = CONFIGURATION.get("ALERT_FOOTER"),
        advertiser: str = "",
    ) -> None:
        
        self.advertiser_id = advertiser_id
        if advertiser == "": 
            advertiser_data = getAdvertiser(advertiser_id)
            advertiser_name = advertiser_data.get("advertiser")
            self.advertiser = advertiser_name

        self.alert_hash = alert_hash
        self.alert_data_link = alert_data_link
        self.entity_id = entity_id
        self.entity_type = entity_type
        self.alert_type = alert_type

        self.alert_id = None
        self.alert_data = None
        self.escalation_configuration = escalation_configuration
        self.default_receiver = default_receiver
        self.alert_message = alert_message.replace("<<ADVERTISER_ID>>", str( self.advertiser_id )).replace("<<ADVERTISER>>", self.advertiser)
        self.alert_subject = alert_subject.replace("<<ADVERTISER_ID>>", str( self.advertiser_id )).replace("<<ADVERTISER>>", self.advertiser)
        self.alert_subtext = alert_subtext
        self.alert_header = alert_header
        self.alert_footer = alert_footer

        self.bq_handler = BigQuery()        
        self.alert_metadata_table = CONFIGURATION.get("ALERT_DESTINATION_TABLE")



    # Calculate Escalation Level
    # -- If alert not found then return ESACALTION_LEVEL: 1
    # -- if alert found and is deactivated return ESCALATION_LEVEL: -1
    # -- Fetch CURRENT_ESCALATION_LEVEL 
    # -- -- Any ESCALATION_LEVEL is valid only if it is greater than CURRENT_ESCALATION_LEVEL
    # -- Compare current and alert timestamp
    # -- Sort Escalation_Configuration by hours_since_generation
    # -- If ESCALATION_LEVEL_HOURS(n) > timestamp differnece > ESCALATION_LEVEL_HOURS(n+1) then return ESCALATION_LEVEL: N
    # -- If timestamp_diff > ESCALATION_LEVEL_HOURS(5) then return ESCALATION_LEVEL: 5
    # -- ESCALATION_LEVEL: -1 indicates alert not required
    def calculateEscalationLevel(self) -> int:
        '''Calculate Escalation Level'''
        logging.debug(f"[+] Calculating Escalation Level")

        # Return LEVEL: 1 when alert data does not exist
        if self.alert_data == None: 
            logging.debug(f"[+] No Existing Alerts Found")
            logging.debug(f"[+] Escalation Level : 1")
            return 1

        # Return LEVEL: 1 when alert data is empty
        if not self.alert_data: 
            logging.debug(f"[+] No Existing Alerts Found")
            logging.debug(f"[+] Escalation Level : 1")
            return 1

        # Return LEVEL: -1 when alert exists but is deactivated
        if self.alert_data.get("alert_status") == False: 
            logging.debug(f"[+] Alert Deactivated")
            logging.debug(f"[+] Escalation Level : -1")
            return -1
        
        # Fetching Current Escalation Level
        current_escalation_level = self.getCurrentEscalationLevel()

        # Calculating TIME difference between current_timestamp and generation_timestamp
        generation_timestamp = self.alert_data.get( "generation_timestamp" ).replace(tzinfo=timezone("Asia/Calcutta"))
        current_timestamp = datetime.now(timezone("Asia/Calcutta"))

        # Conversion constants
        SECONDS_PER_MINUTE = 60
        MINUTES_PER_HOUR = 60
        SECONDS_PER_HOUR = SECONDS_PER_MINUTE * MINUTES_PER_HOUR
        
        # Calculating HOUR difference between current_timestamp and generation_timestamp
        generation_delta = current_timestamp - generation_timestamp
        generation_delta_hours = generation_delta.seconds / SECONDS_PER_HOUR

        # Sorting Escalation Configuration based on HOURS_TO_ESCALATE
        logging.debug(f"[+] Sorting Escalation Configuration ...")
        escalation_list = [ ( int(key), self.escalation_configuration[key] ) for key in self.escalation_configuration ]
        escalation_compare_function = lambda element_1, element_2 : element_1[1] < element_2[1] 
        escalation_key_function = functools.cmp_to_key(escalation_compare_function)
        sorted_escalaion_levels = sorted( escalation_list, key = escalation_key_function ) 
        logging.debug(f"[+] Escalation Configuration Sorted !!!")
        
        # Checking Final Escalation Level
        # -- If TIME_DIFFERENCE.HOUR > FINAL_LEVEL.HOURS_TO_ESCALATE
        # -- return FINAL_LEVEL.LEVEL
        ( final_level , final_hours_to_escalate ) = sorted_escalaion_levels[-1]
        if generation_delta_hours > final_hours_to_escalate : 
            logging.debug(f"[+] Escalation Level : {final_level}")
            return final_level 

        # Calculating ESCALATION_LEVELS
        # -- for every ESCALATION_LEVEL compute (Should never reach final escalation level because that is already accounted for)
        # -- check if delta_hours > this_level_hours_to_escalate
        # -- check if delta_hours < next_level_hours_to_escalate
        # -- return this_level if both conditions are satisfied
        for (index, element) in enumerate(sorted_escalaion_levels):
            (this_level, this_hours_to_escalate) = sorted_escalaion_levels[index]
            (next_level, next_hours_to_escalate) = sorted_escalaion_levels[index + 1]

            delta_is_greater_than_this = ( generation_delta_hours > this_hours_to_escalate )
            delta_is_lesser_than_next = ( generation_delta_hours < next_hours_to_escalate )

            if delta_is_greater_than_this and delta_is_lesser_than_next: 
                if this_level > current_escalation_level:
                    logging.debug(f"[+] Escalation Level : {this_level}")
                    return this_level
                else:
                    return -1

        # If ESCALATION_LEVEL could not be determined
        return 1



    # Get Alert from Alert Metadata Table
    # -- Send alert data if it was already fetched by another process
    # -- Query alert_metadata table for alert_hash / advertiser_id / alert_type
    # -- If no rows match return row
    # -- If 1 row matches retrun row data
    # -- If multiple rows macth LOG ERROR return last row of data
    def getAlert(self) -> dict:
        '''Get alert from database'''
        logging.debug(f"[+] Fetch Alert")

        # If sned_alert already exits avoid hitting Database
        if self.alert_data != None: return self.alert_data
        
        # Fetch Query
        logging.debug(f"[+] Fetching Alert from Database")
        query = f'''
            SELECT *
            FROM `{self.alert_metadata_table}`
            WHERE 
                alert_hash = "{self.alert_hash}" and 
                advertiser_id = {self.advertiser_id} and
                alert_type = "{self.alert_type}"
        '''

        # Run Query
        data: List[dict] = self.bq_handler.runQuery(query=query)
        
        # Return alert_data
        # If 0 entries are found in database then send {}
        # If 1 entries are found in database then send alert_data
        # If n entries are found in database then send last_entry
        # If n entries are found in databasa then log alert about possible corruption of database
        if len(data) == 0: self.alert_data = {}
        if len(data) == 1: self.alert_data = data[0] 
        if len(data) > 1: 
            logging.error(f"[-] Alert Metadata Table corrupted")
            self.alert_data = data[-1]

        logging.debug(f"[+] Alert Fetched from Database")
        return self.alert_data



    # Get Alert_ID
    # -- If alert_data is not fetched then fetch it
    # -- If alert_data is empty generate a UNIQUE_ID
    # -- If alert_data exists then use its alert_id
    def getAlertId(self) -> str:
        '''Get Alert ID'''
        logging.debug(f"[+] Getting Alert_ID")

        if self.alert_data == None: self.getAlert()
        
        if not self.alert_data: 
            logging.debug(f"[+] Alert does not already exist. Creating New Alert ID")
            self.alert_id = getUniqueId()
            return self.alert_id
        
        logging.debug(f"[+] Alert ID fetched")
        self.alert_id = self.alert_data.get("alert_id")
        return self.alert_id


    def getCurrentEscalationLevel(self) -> str:
        '''Get Current Escalation Level'''
        logging.debug(f"[+] Fetching current escalation level")

        if self.alert_data == None: self.getAlert()

        if not self.alert_data:
            logging.debug(f"[+] Alert does not already exist. Escalation_Level = 0")
            return 0

        logging.debug(f"[+] Escalation Level Fetched")
        return self.alert_data.get("escalation_level")

    # Get Alert Metadata Table Schema
    # -- Get Schema from BigQuery Handler
    def getAlertSchema(self) -> List[SchemaField]:
        '''Get schema of alert metadata table'''
        return self \
            .bq_handler \
            .getTableSchema( self.alert_metadata_table )



    # Set Alert to Alert Metadata Table
    # -- If ESCALATION_LEVEL > 1 delete previous entry database
    # -- load table with genertaed alert_data
    # -- load table with write_disposition as WRITE_APPEND
    def setAlert(self, escalation_level: int = 1) -> None:
        '''Set Alert in Database'''
        logging.debug(f"[+] Setting alert into the database")

        self.getAlertId()

        # Delete previous entry if one already exists
        if escalation_level > 1:
            logging.debug(f"[+] Deleting Previous Entry From the database")
            query = f'''
                DELETE FROM `{self.alert_metadata_table}`
                WHERE alert_id = {self.alert_id}
            '''
            self.bq_handler.runQuery( query = query )

        # Insert row of data into a alert metadata table
        logging.debug(f"[+] Appending new Entiry into the database")
        self.bq_handler.loadTableFromJSON(
            table_name = self.alert_metadata_table,
            json_data = [ self.alert_data ],
            schema = self.getAlertSchema(),
            write_disposition = "WRITE_APPEND"
        )



    # Generate Email Details
    # -- Take Email Details and parse them into json string
    def getAlertEmailDetails(self) -> str:
        '''Get Email Body Details'''
        logging.debug(f"[+] Fetching alert email details")

        email_details = {
            "to" : self.default_receiver,
            "subject" : self.alert_subject.replace("<<ADVERTISER_ID>>", str(self.advertiser_id) ).replace("<<ADVERTISER>>", self.advertiser),
            "subtext" : self.alert_subtext,
            "header" : self.alert_header,
            "footer" : self.alert_footer
        }
        return json.dumps(email_details)



    # Generate Alert 
    # -- Fetch Alert Data if not already fetched
    # -- If ESCALATION_LEVEL = -1 then donot generate any alert
    # -- If ESCALATION_LEVEL = 1 then generate first time alert
    # -- If ESCALATION_LEVEL > 1 then edit existing alert 
    def generateAlertData(self, escalation_level) -> dict: 
        '''Generate Alert Data'''
        logging.debug(f"[+] Generating Alert Data")

        if self.alert_data == None: self.getAlert()

        if escalation_level == -1: return {}
        if escalation_level > 1:
            self.alert_data["escalation_level"] = escalation_level
            self.alert_data["generation_timestamp"] = datetime.strftime( self.alert_data.get("generation_timestamp") , "%Y-%m-%dT%H:%M:%S.%fZ" )
            self.alert_data["alert_message"] = self.alert_message
            self.alert_data["email_details"] = self.getAlertEmailDetails()
            return self.alert_data

        if escalation_level == 1:
            self.alert_id = getUniqueId()
            self.alert_data = {
                "alert_id" : self.alert_id,
                "generation_timestamp" : datetime.now(timezone("Asia/Calcutta")).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "alert_message" : self.alert_message,
                "alert_data_link" : self.alert_data_link,
                "alert_hash" : self.alert_hash,
                "email_details" : self.getAlertEmailDetails(),
                "entity_id" : self.entity_id,
                "affected_entity" : self.entity_type,
                "advertiser_id" : self.advertiser_id,
                "escalation_level" : escalation_level,
                "alert_status" : True,
                "delivery_status" : False,
                "delivery_timestamp" : None,
                "alert_type" : self.alert_type
            }
            return self.alert_data



    # Send Alert
    # -- Fetch alert from database based on ALERT_HASH
    # -- Determine ESCALATION_LEVEL from fetched Data
    # -- If ESCALATION_LEVEL = -1 dont send
    # -- If ESCALATION_LEVEL > 0 then generate alert and send 
    def sendAlert(self) -> bool:
        
        self.getAlert()
        escalation_level = self.calculateEscalationLevel()

        if escalation_level == -1: 
            logging.debug(f"[+] Escalation Level is -1. No Message produced")
            return 0

        self.generateAlertData(escalation_level)
        self.setAlert(escalation_level)

        return self.alert_id