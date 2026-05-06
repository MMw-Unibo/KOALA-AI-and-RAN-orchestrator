import pandas as pd
from os import getenv
from datetime import datetime
from influxdb_client_3 import InfluxDBClient3,write_client_options,SYNCHRONOUS
import argparse

# configuration variables
INFLUX_HOST=getenv("INFLUX_HOST")
INFLUX_TOKEN=getenv("INFLUX_TOKEN")
INFLUX_DB=getenv("INFLUX_DB")

PUSH_INTERVAL=int(getenv("PUSH_INTERVAL","-1"))

def main():
    # reading test dataset
    print("Reading test dataset:",end=" ")
    test_df=pd.read_csv("test.csv")
    test_df["time"]=pd.to_datetime(test_df["time"],unit="ms")
    print("Ok")
    
    # creating the influx client
    print("Creating InfluxDB client:",end=" ")
    wco = write_client_options(write_options=SYNCHRONOUS)
    influx_client = InfluxDBClient3(
        host=f"{INFLUX_HOST}",
        database=f"{INFLUX_DB}",
        # token=f"{INFLUX_TOKEN}",
        write_client_options=wco
        )
    print("Ok")
    
    if PUSH_INTERVAL!=-1:
        # pushing data one by one
        print("Starting to push data to the Influx database")
        for i in range(len(test_df)):
            point=f"{INFLUX_DB} internet={test_df['internet'][i]}"
            influx_client.write(point)
        print("All data pushed to the Influx database")
    else:
        # pushing all data at once
        print("PUSH_INTERVAL set to -1, pushing all data at once:",end=" ")
        influx_client.write_file(file='./test.csv', timestamp_column='time', database=INFLUX_DB, measurement_name="internet", write_precision="ms")
        print("Ok")
if __name__=="__main__":
    main()