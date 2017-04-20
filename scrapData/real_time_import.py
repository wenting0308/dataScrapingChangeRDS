from scrapData.rds_config import *
from scrapData.link_to_mysql import *
import pymysql
import json

# request station data from API
file_in = '../real_time.json'
data_json = open(file_in).read()
station_json = json.loads(data_json) # data type: list

# get connection of MySQL server
conn = connect_to_sql(object)
cur = conn.cursor()

# delete data before insert new data
stm = ('DELETE FROM station_real_time WHERE `number` is not null')
cur.execute(stm)
    
# insert data into table station
insert_stmt = (
    "INSERT INTO station_real_time (number, contract_name, banking, bonus, status, bike_stands, \
    available_bike_stands, available_bikes, last_update_timestamp, last_update_date, data_insert_date)"
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" )

for d in station_json:
    #print(type(d["number"]), d["contract_name"])
    cur.execute(insert_stmt, (d["number"], d["contract_name"], d["banking"], \
                              d["bonus"], d["status"], d["bike_stands"], d["available_bike_stands"], \
                              d["available_bikes"], d["last_update_timestamp"], d["last_update_date"], d["data_insert_date"]))

conn.commit()
cur.close()
conn.close()
print("Done")
