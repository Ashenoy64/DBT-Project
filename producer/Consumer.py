from confluent_kafka import Consumer
from dotenv import load_dotenv
import os

load_dotenv()

KAFAKA_SERVER = os.getenv('KAFKA_CLUSTER_BOOTSTRAP_SERVERS')


consumer = Consumer({'bootstrap.servers': "100.88.89.141:9092,100.88.89.141:9093,100.88.89.141:9094",'group.id': 'reddit', 'auto.offset.reset': 'earliest'})

consumer.subscribe(['reddit'])  

while True:
    message = consumer.poll(1.0)

    if message is None:
        continue
    if message.error():
        print("Consumer error: {}".format(message.error()))
        continue

    print('Received message: {}'.format(message.value().decode('utf-8')))

