
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


DB_CONN_URL = os.environ['DB_CONN_URL']

#Maximum number of records that can be processed at one go
MAX_BATCH_SIZE = 150

ETHERNODES_IP_URL = 'https://ethernodes.org/node/'
USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'
DB_TABLE_NAME = 'ethernodes'

def get_unmapped_cl_ips(engine):
    
    sql_get_unmapped_cl_ips = """
        SELECT ip 
        FROM nodewatch  
        WHERE ip NOT IN ( SELECT ip FROM ethernodes )
    """
    df_unmapped_ips = pd.read_sql(sql_get_unmapped_cl_ips, engine)
    return df_unmapped_ips

def get_ethernodes_data(ip):

    try:
        url =  ETHERNODES_IP_URL + ip
        response = requests.get(url, headers={'User-Agent':USER_AGENT})  

        df = pd.read_html(response.content)[0].reset_index(drop=True)
        df.columns = ['key','val']
        df.set_index('key',inplace=True)

        return df.to_dict()['val']

    except Exception as e:
        logger.info(e)
        return None

def get_el_mapping_from_ethernodes(df_unmapped_ips):
    ethernode_results = []

    logger.info('number of unmapped ips that will be processed in this run %d', min(df_unmapped_ips.shape[0], MAX_BATCH_SIZE))
    for idx, row in df_unmapped_ips.head(MAX_BATCH_SIZE).iterrows():

        logger.info("processing {} of {}".format(idx, min(df_unmapped_ips.shape[0], MAX_BATCH_SIZE)))
        result = {}
        
        ethernode_data = get_ethernodes_data(row['ip'])

        result['ip'] = row['ip']
        result['client_id'] = ethernode_data['Client id'] if ethernode_data is not None else ''
        result['client_name'] = ethernode_data['Client'] if ethernode_data is not None else ''
        result['last_updated'] = datetime.utcnow()

        ethernode_results.append(result)

        time.sleep(1) #sleep before next call

    df_ethernode_data = pd.json_normalize(ethernode_results)
    return df_ethernode_data

def lambda_handler(event, context):
    try: 
        logger.info('service starting..')
        engine = create_engine(DB_CONN_URL, pool_recycle=3600)

        df_unmapped_ips = get_unmapped_cl_ips(engine)
        logger.info('number of unmapped ips %d', df_unmapped_ips.shape[0])
        

        df_ethernode_data = get_el_mapping_from_ethernodes(df_unmapped_ips)
        logger.info('mapping found for %d', df_ethernode_data[df_ethernode_data.client_name != ''].shape[0])

        df_ethernode_data.to_sql(name=DB_TABLE_NAME, con=engine, if_exists = 'append', index=False)
        logger.info('service completed successfully')
                    
    except Exception as e:
        logger.info(e)
        logger.info('service aborted')


if __name__ == "__main__":
    lambda_handler(None, None)
