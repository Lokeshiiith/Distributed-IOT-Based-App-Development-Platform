from kafka import KafkaConsumer
from time import sleep
import json
import threading
from kafka.errors import KafkaError
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# Variable declarations
kafkaIp = "<your_kafka_ip>"
kafkaPortNo = "<your_kafka_port>"
kafkaGroupId = "log-group"
kafkaTopicName = ["log-monitoring-fault-tolerance", "log-deployer", "log-sensor-manager", "log-load-balancer", "log-node-manager", "log-scheduler", "log-validator-workflow", "log-bootstrapper", "log-api-gateway", "log-ldap"]

def storeLogs(fileName):
    loggingCollection = logger_DB[fileName]
    while True:
        try:
            # Retrieve logs from the collection
            logs = list(loggingCollection.find())
            # Write logs to a text file
            with open(fileName, "w") as f:
                for log in logs:
                    message = f"{log['subsystem_name']} : {log['container_up_time']} : {log['severity']} : {log['date']} {log['time']} => {log['message']}"
                    f.write(f"{message}\n")

            # Upload the text file to Azure Blob Storage
            connection_string = "<your_connection_string>"
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            container_client = blob_service_client.get_container_client("log-storage")
            blob_client = container_client.get_blob_client(fileName)

            with open(fileName, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)

            sleep(60)
        except Exception as e:
            print(f"An error has occurred while uploading logs to the blob storage: {e}")

def logSystem(kafkaTopicName):
    consumer = KafkaConsumer(kafkaTopicName, group_id=kafkaGroupId, bootstrap_servers=[f"{kafkaIp}:{kafkaPortNo}"])
    try:
        for message in consumer:
            messageContents = json.loads(message.value)
            loggingCollection = logger_DB[f"log-{messageContents['subsystem_name']}"]
            loggingCollection.insert_one(messageContents)
    except KeyboardInterrupt:
        pass
    except KafkaError as kError:
        print(f"Error while consuming messages from topics {kafkaTopicName}: {kError}")
    sleep(10)

for topic in kafkaTopicName:
    t1 = threading.Thread(target=logSystem, args=(topic,))
    t1.start()
    t2 = threading.Thread(target=storeLogs, args=(topic,))
    t2.start()
