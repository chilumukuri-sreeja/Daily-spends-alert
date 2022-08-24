import json
import base64
from main import IO_Daily_Budget_Alert
event = {
    "table_name" : "949727698",
    "destination_table" : "nyo-yoptima.sdf_insertion_order.949727698"
} 

event = json.dumps( event ).encode( "ascii" )
event = base64.b64encode( event )

IO_Daily_Budget_Alert( { "data" : event }, None )