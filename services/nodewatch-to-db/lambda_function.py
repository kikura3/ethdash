import pymysql
from sqlalchemy import create_engine
import pandas as pd
import pymongo
from datetime import datetime
import logging
import os


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


MONGO_URL = os.environ['MONGO_URL']
DB_CONN_URL = os.environ['DB_CONN_URL']

DB_TABLE_NAME = 'nodewatch'

#mapping from nodewatch cols to database cols
NODE_WATCH_TO_DB_COLS = {
    'node_id' : 'node_id',
    'pubkey' : 'pubkey',
    'ip' : 'ip',
    'tcp_port' : 'tcp_port',
    'udp_port' : 'udp_port',
    'user_agent_raw' : 'user_agent_raw',
    'score' : 'score',
    'is_connectable_bool' : 'is_connectable',
    'last_connected_dt' : 'nw_last_connected',
    'last_updated_dt' : 'nw_last_updated',
    'user_agent.name' : 'user_agent_name',
    'user_agent.version' : 'user_agent_version',
    'user_agent.os' : 'user_agent_os',
    'geo_location.asn.name' : 'geo_location_name',
    'geo_location.asn.domain' : 'geo_location_domain',
    'geo_location.asn.type' : 'geo_location_type',
    'geo_location.country' : 'geo_location_country',
    'geo_location.state' : 'geo_location_state',
    'geo_location.city' : 'geo_location_city',
    'geo_location.latitude' : 'geo_location_latitude',
    'geo_location.longitude' : 'geo_location_longitude',
    'sync.status.bool' : 'sync_status',
}

def get_last_nw_read_time(engine):
    """
    retrieves the last read time from nodewatch server
    """
    try:
        sql_read_nodewatch_last_updated = """
        SELECT 
            COALESCE(MAX(nw_last_updated),'1970-01-01 00:00:00') AS last_updated
        FROM nodewatch;
        """
        last_updated = pd.read_sql(sql_read_nodewatch_last_updated, engine).iloc[0]['last_updated']
        return datetime.strptime(last_updated, '%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logger.error(e)
        return datetime.strptime('1970-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')

def get_changes(df, last_nw_read_time):
    delta_mask = ((df.is_connectable) &
                  (df.last_updated > last_nw_read_time.timestamp()))
    df_to_be_loaded = df[delta_mask][NODE_WATCH_TO_DB_COLS.keys()]
    df_to_be_loaded.rename(columns=NODE_WATCH_TO_DB_COLS, inplace=True)
    df_to_be_loaded['last_updated'] = datetime.utcnow()
    return df_to_be_loaded

def get_nodewatch_data():
    conn = pymongo.MongoClient(MONGO_URL, unicode_decode_error_handler='ignore')
    db = conn['crawler']['peers']
    df = pd.json_normalize(list(db.find({'is_connectable':True})))
    df['is_connectable_bool'] = df['is_connectable'].map(lambda x: 'Y' if x else 'N')
    df['sync.status.bool'] = df['sync.status'].map(lambda x: 'Y' if x else 'N')
    
    df['last_connected_dt'] = df['last_connected'].map(lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
    df['last_updated_dt'] = df['last_updated'].map(lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
    return df

def lambda_handler(event, context):
    try: 
        logger.info('service starting..')
        engine = create_engine(DB_CONN_URL, pool_recycle=3600)

        #get all nodewatch data
        df_nodewatch_data = get_nodewatch_data()

        #get last run time
        last_nw_read_time = get_last_nw_read_time(engine)

        #get changes to be loaded to database
        df_updates = get_changes(df_nodewatch_data, last_nw_read_time)
        logger.info('rows to be updated: %d', df_updates.shape[0])

        df_updates.to_sql(name=DB_TABLE_NAME, con=engine, if_exists = 'replace', index=False)
        logger.info('service completed successfully')
                    
    except Exception as e:
        logger.info(e)
        logger.info('service aborted')

if __name__ == "__main__":
    lambda_handler(None, None)