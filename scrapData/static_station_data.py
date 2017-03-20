from scrapData.rds_config import *
from scrapData.link_to_mysql import *
import pymysql
import json

# request station data from API
file_in = '../Dublin.json'
data_json = open(file_in).read()
station_json = json.loads(data_json) # data type: list

# get connection of MySQL server
conn = connect_to_sql(object)
cur = conn.cursor()

# delete data before insert new data
stm = ('DELETE FROM stations WHERE `number` is not null')
cur.execute(stm)
    
# insert data into table station
insert_stmt = (
    "INSERT INTO stations (number, name, address, latitude, longitude)"
    "VALUES (%s, %s, %s, %s, %s)" )

for d in station_json:
    cur.execute(insert_stmt, (d["number"], d["name"], d["address"], d["latitude"], d["longitude"]) )

conn.commit()
cur.close()
conn.close()
print("Done")
