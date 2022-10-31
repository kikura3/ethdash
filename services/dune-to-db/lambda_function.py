
from requests import get, post
import pandas as pd
import logging
import pymysql
from sqlalchemy import create_engine
import time
from dune_api import execute_query, get_query_status, get_query_results, execute_query_with_params, cancel_query_execution
import os

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DB_CONN_URL = os.environ['DB_CONN_URL']
ENGINE = create_engine(DB_CONN_URL, pool_recycle=3600)

#define query_id => table mapping (along with expected run time and max wait time in minutes)
QUERY_MAP = {
    '1300491' : {
        'table_name': 'dn_validators_signup_weekly',
        'expected_runtime_min' : 5,
        'max_waittime_min': 10, 
        'append' : False,
        'params' : False
    },
    '1467530' : {
        'table_name': 'dn_validators',
        'expected_runtime_min' : 5,
        'max_waittime_min': 15, 
        'append' : True,
        'params' : True
    },
    '1467978' : {
        'table_name': 'dn_validators_legacy',
        'expected_runtime_min' : 1,
        'max_waittime_min': 5, 
        'append' : True,
        'params' : True
    },
    '1471184' : {
        'table_name': 'dn_depositor_labels',
        'expected_runtime_min' : 5,
        'max_waittime_min': 15, 
        'append' : False,
        'params' : False
    },
    '1481765' : {
        'table_name': 'dn_depositor_info',
        'expected_runtime_min' : 5,
        'max_waittime_min': 15, 
        'append' : False,
        'params' : False
    }
}

def get_latest_block_from_dn_validators():
    try:
        sql_last_read_block = """
            SELECT MAX(block_number) AS latest_block
            FROM dn_validators
        """
        latest_block = int(pd.read_sql(sql_last_read_block, ENGINE).iloc[0]['latest_block'])
        
        return latest_block
    except Exception as e:
        return None

def get_latest_block_from_dn_validators_legacy():
    try:
        sql_last_read_block = """
            SELECT MAX(block_number) AS latest_block
            FROM dn_validators_legacy
        """
        latest_block = int(pd.read_sql(sql_last_read_block, ENGINE).iloc[0]['latest_block'])
        return latest_block
    except Exception as e:
        return None

def form_query_params(query_id):

    params = {}

    if query_id == '1467530':
        start_block = get_latest_block_from_dn_validators()
        if start_block is None:
            start_block = 11052984 #contract deployment block

        params['start_block'] = start_block + 1

    if query_id == '1467978':
        start_block = get_latest_block_from_dn_validators_legacy()
        if start_block is None:
            start_block = 11052984 #contract deployment block

        params['start_block'] = start_block + 1

    return params

def run_query(query_id, expected_minutes=5, max_wait_minutes=10, params=None):

    t_end = time.time() + 60 * max_wait_minutes

    if params:
        query_params = form_query_params(query_id)
        logger.info('params: {}'.format(query_params))
        execution_id = execute_query_with_params(str(query_id), query_params)
    else:    
        execution_id = execute_query(str(query_id))
        
    logger.info('executed {}'.format(query_id))
    while time.time() < t_end:
        logger.info('checking status for {}'.format(execution_id))
        query_status = get_query_status(execution_id).json()
        logger.info(query_status)
        if query_status['state'] == 'QUERY_STATE_COMPLETED':
            break
        
        logger.info('sleeping...')
        time.sleep(expected_minutes/2 * 60) 

    logger.info(query_status)

    if query_status['state'] != 'QUERY_STATE_COMPLETED':
        query_status = get_query_status(execution_id).json()
        if query_status['state'] != 'QUERY_STATE_COMPLETED':
            cancel_query_execution(execution_id)
            raise Exception("query took too long")

    response = get_query_results(execution_id)
    df_data = pd.DataFrame(response.json()['result']['rows'])
    return df_data


def lambda_handler(event, context):

    engine = create_engine(DB_CONN_URL, pool_recycle=3600)
    query_ids = event["query_ids"].split(",")

    for query_id in query_ids:
        logger.info("query_id {}".format(query_id))
        query_info = QUERY_MAP[query_id]
        
        df_data = run_query(query_id, query_info['expected_runtime_min'], query_info['max_waittime_min'], query_info['params'])
        logger.info('total number of records fetched for {} : {}'.format(query_info['table_name'], df_data.shape[0]))
        df_data.to_sql(name=query_info['table_name'], con=engine, if_exists = 'append' if query_info['append'] else 'replace', index=False)
