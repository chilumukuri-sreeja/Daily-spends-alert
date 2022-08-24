import uuid
from pandas import DataFrame
import pandas as pd

from config import CONFIGURATION
from BigQuery.BigQuery import BigQuery

# Get Unique Integer ID
# -- Generated from UUID 
# -- Maximum 128bit size supported
def getUniqueId(nbits: int = 32) -> int:
    
    if nbits > 128: nbits = 128
    if nbits < 0: nbits = 0

    # Calculate nBit Id
    # -- nBit = 3 => (2^3 - 1) = 7 = (111)
    unique_id = uuid.uuid4().int
    masking_bits = (( 1 << nbits ) - 1)
    final_id = unique_id & masking_bits

    return final_id

# Calculate DataFrame Hash
# -- Calculate hash for every row removing index
# -- Sum up individual hashes 
# -- Return final hash
def getDataFrameHash(dataframe: DataFrame) -> int:
    hash = pd \
        .util \
        .hash_pandas_object(dataframe, index=False) \
        .sum()

    hash = int(hash)
    return hash if hash > 0 else -1 * hash

# Get Advertiser Name for given Advertiser_ID
# -- Query nyo-yoptima.metadata.advertiser_masterlist to get mapping
def getAdvertiser(advertiser_id: int) -> dict:

    project_id = CONFIGURATION.get("PROJECT_ID")
    advertiser_masterlist = CONFIGURATION.get("ADVERTISER_MASTERLIST")

    bq_handler = BigQuery()
    query = f'''
        SELECT *
        FROM `{project_id}.{advertiser_masterlist}`
        WHERE advertiser_id = {advertiser_id}
    '''

    advertiser_list = bq_handler.runQuery(query = query)
    if len(advertiser_list) == 0: raise Exception("Advertiser Not Found")

    advertiser = advertiser_list[0]

    return advertiser
