from scrapData.real_time_station_data import *
import requests
import json
import time
import traceback

NAME="Dublin" # name of contract
STATIONS="https://api.jcdecaux.com/vls/v1/stations" # JCDecaux endpoint
APIKEY = "0e608dd7e501832869cb2b7b88fb066c1bcbe277"

def main():
    
    # run forever...
    while True:
        try:
            r = requests.get(STATIONS, params={"apiKey": APIKEY, "contract": NAME})
            
            # insert data to database here: pass list to function store
            # if data already exists then raise except (or other error occur
            try:
                store(json.loads(r.text))
            # duplicate data insert error
            except pymysql.err.IntegrityError:
                print("Error: Try to insert duplicate data, wait for next run....")
                time.sleep(5*60)
            except:
                print("Error: Other insert error, wait for next run....")
                time.sleep(5*60)
            else:
                # now sleep for 5 minutes
                print("Wait for next run....")
                time.sleep(5*60)
        except:
            # if there is any problem, print the traceback
            # append traceback information to file --> TBD
            print(traceback.format_exc())
    return


if __name__ == '__main__':
    main()