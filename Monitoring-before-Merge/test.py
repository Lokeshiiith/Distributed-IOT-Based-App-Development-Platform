from kafka import KafkaConsumer
from kafka import KafkaConsumer
import json
import threading
import time
from time import sleep
import logging
import global_file
import sys
from pprint import pprint

# Set the necessary configurations
kafkaIp = "0.0.0.0"
kafkaPortNo = "9092"
kafkaTopicName = 'heartbeatMonitoring'
kafkaGroupId = 'my-group-id'

# Create a KafkaConsumer instance
consumer = KafkaConsumer(
    kafkaTopicName,
    group_id=kafkaGroupId,
    bootstrap_servers=[f"{kafkaIp}:{kafkaPortNo}"],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Consume messages from the Kafka topic
for message in consumer:
    # print(f"Received message: {message}")
    pprint(message)


['"Load_balancer', '600', 'localhost', 'uname', 'pswd', '1681111256.6866066"']
