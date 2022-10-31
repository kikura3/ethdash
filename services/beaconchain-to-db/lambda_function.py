
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from datetime import datetime
import requests
import pandas as pd
import logging
import time
import os

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

#Target database connection info
DB_CONN_URL = os.environ['DB_CONN_URL']
ENGINE = create_engine(DB_CONN_URL, pool_recycle=3600)


VALIDATOR_INFO_URL_TMPL = "https://beaconcha.in/api/v1/validator/{}"
MAX_VALIDATOR_INDEX_PER_REQUEST = 100
VALIDATOR_INFO_TABLE_NAME = 'validator_info'

VALIDATOR_PERFORMANCE_URL_TMPL = "https://beaconcha.in/api/v1/validator/{}/performance"
MAX_VALIDATOR_INDEX_PER_REQUEST = 100
VALIDATOR_PERFORMANCE_TABLE_NAME = 'validator_performance'


def get_last_validator_index(table_name):

    try:
        sql_validator_index = """
        SELECT MAX(validatorindex) AS validator_index
        FROM {}
        """.format(table_name)
        latest_validator_index = int(pd.read_sql(sql_validator_index, ENGINE).iloc[0]['validator_index'])
        return latest_validator_index

    except Exception as e:
        logger.info(e)
        return None

def get_validator_data_from_bc(validator_table_name, start_validator_index):
    try:
        
        validator_index = "," .join([str(n) for n in list(range(start_validator_index, start_validator_index + MAX_VALIDATOR_INDEX_PER_REQUEST))])
        logger.info("collecting info for 100 validators starting from {}...".format(start_validator_index))

        validator_index_encoded = requests.utils.quote(validator_index)

        if validator_table_name == VALIDATOR_INFO_TABLE_NAME:
            validator_data_url = VALIDATOR_INFO_URL_TMPL.format(validator_index_encoded)
        else:
            validator_data_url = VALIDATOR_PERFORMANCE_URL_TMPL.format(validator_index_encoded)

        r = requests.get(validator_data_url)
        return pd.DataFrame(r.json()['data'])

    except Exception as e:
        logger.info(e)
        return None

def collect_update_validator_data(validator_table_name, start_validator_index, max_run_time_min=4):

    t_end = time.time() + 60 * max_run_time_min

    while time.time() < t_end:

        df_validator_data = get_validator_data_from_bc(validator_table_name, start_validator_index)
        if df_validator_data is not None:
            df_validator_data.to_sql(name=validator_table_name, con=ENGINE, if_exists = 'append', index=False)

        start_validator_index = start_validator_index + MAX_VALIDATOR_INDEX_PER_REQUEST 
        time.sleep(6) #10 requests allowed per minute

def lambda_handler(event, context):
    try: 
        logger.info('service starting..')

        table_name = event["table_name"]

        last_validator_index = get_last_validator_index(table_name)

        start_validator_index = 0 if last_validator_index is None else last_validator_index + 1
        
        collect_update_validator_data(table_name, start_validator_index)
       
        logger.info('service completed successfully')
                    
    except Exception as e:
        logger.info(e)
        logger.info('service aborted')
