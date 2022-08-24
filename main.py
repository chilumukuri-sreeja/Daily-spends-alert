import json
import base64
import logging
from datetime import datetime
from pytz import timezone
from pathlib import Path

import pandas as pd

from Utility.Utility import getDataFrameHash, getUniqueId
from BigQuery.BigQuery import BigQuery
from GoogleCloudStorage.GoogleCloudStorage import GoogleCloudStorage
from Pubsub.PubSub import PubSub
from  Alert.Alert import Alert
from config import CONFIGURATION


# IO_required_daily_spends Alert
# -- Runs for every updated SDF (Trigged from cloud-pubsub)
# -- Checks if pacing amount is not fixed as per required_daily_spends
def IO_Daily_Budget_Alert(event, context):
    
    # Fetch Pubsub trigger message 
    # -- event has json payload sent by pubsub-trigger
    # -- extract advertiser_id and sdf_table name from payload
    if "data" in event:
        payload = base64.b64decode(event['data']).decode('utf-8')
        payload = json.loads(payload)
    advertiser_id = int(payload.get("table_name"))
    #sdf_table = payload.get("destination_table")


    # Run Query on BQ to check pacing_amount is fixed as per required_daily_spends Anomaly
    # -- Initialise BigQuery
    # -- Initialise Alert Data
    bq_handler = BigQuery()
    alert_data = []
    QUERY = f'''
    select 
      io_masterlist.insertion_order_id as IO_id,
      io_masterlist.insertion_order as IO_Name,
      CONCAT(FORMAT("%'d", CAST(io_masterlist.daily_max_micros / pow(10, 6) as INT64))) as Daily_IO_Budget,
      CONCAT(FORMAT("%'d",cast(( segment_spends.total_budget - segment_spends.budget_spent ) / ( date_diff( date( segment_spends.end_date ) , current_date(), DAY )+1) as INT64))) as Required_Daily_Budget,
      case 
          when 
          io_masterlist.daily_max_micros / pow(10, 6) > 2 * ( segment_spends.total_budget - segment_spends.budget_spent ) / ( date_diff( date( segment_spends.end_date ) , current_date(), DAY )+1) Then 'OverBudgeted'
          ELSE 'UnderBudgeted'
          END AS Daily_Budget_Status
from (
  select 
    spends.Insertion_Order_ID, 
    sum( spends.Spends ) as budget_spent,
    avg( budget_segments.Total_Budget ) as total_budget,
    min( budget_segments.start_date ) as start_date,
    max( budget_segments.end_date ) as end_date
  from `deeplake.deepm.dv3_costs_spends` as spends
  left join (
    select 
      advertiser_id,
      campaign_id, 
      insertion_order_id,
      status,
      start_date, 
      end_date,
      budget_amount_macros / pow(10, 6) as Total_Budget
    from `nyo-yoptima.metadata.insertion_order_budget_segment`
    where 
      current_date()-1 >= date( start_date ) and
      current_date()-1 <= date( end_date ) and
      advertiser_id in ( select distinct advertiser_id from `nyo-yoptima.metadata.advertiser_masterlist` where status = "ENTITY_STATUS_ACTIVE" )
  ) as budget_segments
  on  
    spends.Insertion_Order_ID = budget_segments.insertion_order_id
  where
    spends.Date >= date( budget_segments.start_date ) and
    spends.Date <= date( budget_segments.end_date )
  group by
    spends.Insertion_Order_ID
) as segment_spends
left join `nyo-yoptima.metadata.insertion_order_masterlist` as io_masterlist
on segment_spends.insertion_order_id = io_masterlist.insertion_order_id
where
  (
  io_masterlist.daily_max_micros / pow(10, 6) > 2 * ( segment_spends.total_budget - segment_spends.budget_spent ) / ( date_diff( date( segment_spends.end_date ) , current_date(), DAY )+1) or
io_masterlist.daily_max_micros / pow(10, 6) < 0.5 * ( segment_spends.total_budget - segment_spends.budget_spent ) / ( date_diff( date( segment_spends.end_date ) , current_date(), DAY )+1)
  ) and status = "ENTITY_STATUS_ACTIVE" 
  and io_masterlist.advertiser_id = {advertiser_id}
  order by (
        case 
          when 
          io_masterlist.daily_max_micros / pow(10, 6) > 2 * ( segment_spends.total_budget - segment_spends.budget_spent ) / ( date_diff( date( segment_spends.end_date ) , current_date(), DAY )+1) Then 'OverBudgeted'
          ELSE 'UnderBudgeted'
        end
      )
    '''
    try: alert_data = bq_handler.runQuery(QUERY)
    except Exception as error:
        return json.dumps({ "Message" : "Error Running Query", "error" : str(error) }), 500
    
    if len(alert_data) == 0:
        logging.debug(f"[+] No messages to alert")
        return json.dumps({ "Message" : "No Alerts to Report" }), 200


    # Obtain DataFrame from Bigquery Data
    # -- Calculate dataframe hash
    alert_dataframe = pd.DataFrame(alert_data)
    dataframe_hash = getDataFrameHash(alert_dataframe)


    # Write DataFrame to CSV
    file_name = f"{advertiser_id}_{dataframe_hash}.csv"
    csv_path = Path(CONFIGURATION.get("DATA_DIRECTORY")) / file_name
    alert_dataframe.to_csv(csv_path, index=False)
    logging.debug(f"[+] File Downloaded to {csv_path}")


    # Write Data to Google Cloud Storage
    base_bucket = CONFIGURATION.get("UPLOAD_BUCKET")
    alert_directory = CONFIGURATION.get("UPLOAD_PATH")
    gcs_handler = GoogleCloudStorage()
    gcs_handler.uploadFile(
        bucket_name = base_bucket,
        blob_name = f"{alert_directory}/{file_name}",
        filepath = csv_path
    )


    # Initiailse Alert 
    alert_handler = Alert(
        advertiser_id = advertiser_id,
        alert_hash = dataframe_hash,
        alert_data_link = f"gs://{base_bucket}/{alert_directory}/{file_name}",
        entity_type = "Advertiser"
    )
    alert_id = alert_handler.sendAlert()
    if alert_id == 0: 
        logging.debug(f"[+] Escalation Level = 0. Aborting Operation.")
        return
    
    # Sending Message through pubsub
    # -- Initialise PubSub
    # -- Publish Message with alert_id attribute
    pubsub_handler = PubSub()
    attributes = {
        "alert_id" : str(alert_id),
        "advertiser_id" : str(advertiser_id)
    }
    pubsub_handler.publishMessage(message = f"{CONFIGURATION.get('ALERT_TYPE')} raised for {advertiser_id}", attribute = attributes)