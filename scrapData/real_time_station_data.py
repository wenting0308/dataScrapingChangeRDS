from scrapData.rds_config import *
from scrapData.link_to_mysql import *

import pymysql
import json
import time
import datetime
import pickle

def store(request_data):
    
    # get connection of MySQL server
    conn = connect_to_sql(object)
    cur = conn.cursor()
    
    runtime = str(datetime.datetime.now())
        
    # insert data into table station
    insert_stmt = (
        "INSERT INTO stations_real_time (number, contract_name, banking, bonus, status, bike_stands, \
            available_bike_stands, available_bikes, last_update_timestamp, last_update_date, data_insert_date)"
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" )
   
    for d in request_data:
        
        # convert timestamp to datetime
        epoch = datetime.datetime.utcfromtimestamp(d["last_update"]/1000.0)

        # insert data to table stations_real_time
        cur.execute(insert_stmt, (d["number"], d["contract_name"], d["banking"], d["bonus"], d["status"],
                                  d["bike_stands"], d["available_bike_stands"], d["available_bikes"], 
                                  d["last_update"], epoch, runtime) )
    
    conn.commit()
    cur.close()
    conn.close()
    
    # store data  to local machine
    pickle_name = '../backup/station_real_time_' + runtime + '.pkl'
    pickle_json = open(pickle_name, 'wb')
    pickle.dump(request_data, pickle_json)
    pickle_json.close()
    
    print("Runtime:" , runtime, " data insert success....")