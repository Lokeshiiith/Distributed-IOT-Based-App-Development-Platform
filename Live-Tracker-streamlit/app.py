import streamlit as st
from pymongo import MongoClient
import pymongo
import pandas as pd
import threading
import time
# Connect to MongoDB

mongo = 'mongodb+srv://ias:ias@cluster0.xcykxcz.mongodb.net/'
client = pymongo.MongoClient(mongo)

project_db = client["IAS_PROJECT"]
collection_standardMonitoring = project_db["standardMonitoring"]
collection_alertedMonitoring = project_db["alertedMonitoring"]

def update_table(collection_standardMonitoring, collection_alertedMonitoring):
    # make a dataframe
    Column_names = ["Container Name", "Node_name","Status","Heartbeat Since"]
    df = pd.DataFrame(columns = Column_names)
    # Iterate over the documents in collection_standardMonitoring
    for i in collection_standardMonitoring.find():
        heartbeat_since = int(time.time()) - int(i['current_time']) 
        df = pd.concat([df, pd.DataFrame({
            "Container Name": i["container_name"],
            "Node_name": i["node_name"],
            "Status": "active",  # Assign 'active' status for data from collection_standardMonitoring
            "Heartbeat Since": heartbeat_since
        }, index=[0])], ignore_index=True)

    # Iterate over the documents in collection_alertedMonitoring
    for i in collection_alertedMonitoring.find():
        heartbeat_since = int(time.time()) - int(i['current_time']) 
        df = pd.concat([df, pd.DataFrame({
            "Container Name": i["container_name"],
            "Node_name": i["node_name"],
            "Status": "alert",  # Assign 'alert' status for data from collection_alertedMonitoring
            "Heartbeat Since": heartbeat_since
        }, index=[0])], ignore_index=True)
    st.write(df)
    time.sleep(10) 
    
# thread = threading.Thread(target=update_table, args=(collection_standardMonitoring, collection_alertedMonitoring))
# thread.start()
while True:
    update_table(collection_standardMonitoring, collection_alertedMonitoring)
    time.sleep(10)





# main funtion in puytjon
if __name__ == "__main__":
    update_table(collection_standardMonitoring, collection_alertedMonitoring)
    time.sleep(10)