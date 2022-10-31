
import tarfile
import os
import requests
import pandas as pd
import logging
import pymysql
from sqlalchemy import create_engine
import time
import geoip2.database
import ipaddress


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DB_CONN_URL = os.environ['DB_CONN_URL']
GEODB_LICENSE_KEY = os.environ['GEODB_LICENSE_KEY']
GEODB_URL = 'https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-ASN&license_key={}&suffix=tar.gz'.format(GEODB_LICENSE_KEY)

ENGINE = create_engine(DB_CONN_URL, pool_recycle=3600)
DB_TABLE_NAME='nodewatch_geo_info'

def get_nodewatch_data():

    sql_nodewatch = "SELECT * FROM nodewatch"
    df_nodewatch = pd.read_sql(sql_nodewatch, ENGINE)
    return df_nodewatch

def get_geo_reader():

    r = requests.get(GEODB_URL, allow_redirects=True)

    download_file_name = 'geodb.tar.gz'

    with open('geodb.tar.gz', 'wb') as f:
        f.write(r.content)

    tar = tarfile.open('geodb.tar.gz', "r:gz")
    tar.extractall()
    tar.close()

    fname = [os.path.join(d,f)  for d in os.listdir('.') if 'Geo' in d for f in os.listdir(d) if 'Geo' in f][0]
    geoip_reader = geoip2.database.Reader(fname)

    return geoip_reader

def find_geo_name_from_db(x, reader):

    try:
        response = reader.asn(x)
        return [x, response.autonomous_system_number, response.autonomous_system_organization.replace(",","-")]
    except Exception as e:
        return [x, None, None]
    
def lambda_handler(event, context):

    logger.info("service starting...")
    df_nodewatch = get_nodewatch_data()
    geo_reader = get_geo_reader()

    result = df_nodewatch.ip.map(lambda x: find_geo_name_from_db(x, geo_reader))
    df_result = pd.DataFrame([r for r in result])
    df_result.columns = ['ip','asn','asn_name']

    df_result.to_sql(name=DB_TABLE_NAME, con=ENGINE, if_exists = 'replace', index=False)
    logger.info("results updated for {}".format(df_result.shape[0]))
