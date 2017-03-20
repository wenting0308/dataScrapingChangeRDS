from scrapData.rds_config import *
from scrapData.link_to_mysql import *
import sys
import logging
import pymysql
import requests
import json
import time
import datetime

def connect_to_sql(object):
    rds_db = rds_config()
    rds_db.db_config_setup()
    
    rds_host = rds_db.db_endpoint
    name = rds_db.db_username
    password = rds_db.db_password
    db_name = rds_db.db_name
    port = 3306
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    try:
        conn = pymysql.connect(rds_host, port = port, user=name,
                               passwd=password, db=db_name, connect_timeout=20)
    except:
        logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
        sys.exit()
        
    else:
        print("[",str(datetime.datetime.now()), "] Finished link to RDS...")
        return conn
