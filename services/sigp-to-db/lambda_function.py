
import requests
import pymysql
from sqlalchemy import create_engine
import logging
import pandas as pd
from datetime import datetime
import os

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DB_CONN_URL = os.environ['DB_CONN_URL']
API_USERNAME= os.environ['API_USERNAME']
API_PASSWORD= os.environ['API_PASSWORD']

URL= 'https://api.blockprint.sigp.io/blocks/'
SEED_START_SLOT=0 #enter a reasonable recent slot number as seed

DB_TABLE_NAME = 'blockprint'
BLOCKPRINT_DB_COLS_MAP = {
    'proposer_index' : 'proposer_index',
    'slot' : 'slot',
    'best_guess_single' : 'predicted_client'
}

def get_last_read_slot(engine):
    sql_last_read_slot = """
        SELECT MAX(slot) AS last_slot
        FROM blockprint
    """
    last_slot = pd.read_sql(sql_last_read_slot, engine).iloc[0]['last_slot']
    return last_slot

def get_blockprint_data(start_slot):
    try:
        blockprint_url = 'https://api.blockprint.sigp.io/blocks/{}'.format(start_slot)
        logger.info(blockprint_url)
        r = requests.get(blockprint_url, auth=( API_USERNAME, API_PASSWORD ))
        df_data = pd.DataFrame(r.json())
        return df_data
    except Exception as e:
        logger.info(e)
        return None
    
def lambda_handler(event, context):
    try: 
        logger.info('service starting..')
        engine = create_engine(DB_CONN_URL, pool_recycle=3600)

        #get the last read slot from blockprint
        last_slot = get_last_read_slot(engine)

        #get blockprint client prediction from the last slot till latest
        df_blockprint_data = get_blockprint_data(last_slot + 1)

        #get changes to be loaded to database
        df_data_to_db = df_blockprint_data[BLOCKPRINT_DB_COLS_MAP.keys()].rename(columns=BLOCKPRINT_DB_COLS_MAP)
        df_data_to_db['last_updated'] = datetime.utcnow()

        df_data_to_db.to_sql(name=DB_TABLE_NAME, con=engine, if_exists = 'append', index=False)

        logger.info('service completed successfully')
                    
    except Exception as e:
        logger.info(e)
        logger.info('service aborted')

if __name__ == "__main__":
    lambda_handler(None, None)
