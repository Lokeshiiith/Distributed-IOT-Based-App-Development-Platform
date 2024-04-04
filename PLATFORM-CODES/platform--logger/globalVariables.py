import os
from pymongo import MongoClient

node_name = os.getenv('node_name')
container_name = os.getenv('container_name')
container_up_time = os.getenv('container_up_time')

MONGO_CONNECTION_URL = "mongodb+srv://ias:ias@cluster0.xcykxcz.mongodb.net/"
mongoClient = MongoClient(MONGO_CONNECTION_URL)


service_Registry_DB = mongoClient["IAS_PROJECT"]
service_Registry_Collection = service_Registry_DB["service_registry"]

kafka_Container_Details = service_Registry_Collection.find_one({'app_name': 'platform', 'service_name' : 'kafka'})


logger_DB = mongoClient["LOGS"]
