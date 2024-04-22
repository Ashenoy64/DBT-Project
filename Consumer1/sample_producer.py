import time
import json

from kafka import KafkaProducer

producer = KafkaProducer(
    client_id="sample_prod",
    bootstrap_servers="localhost:9092")

while True:
    try:
        producer.send("topic-t1", json.dumps({'title':'bipartisan'}).encode('utf-8'))
        time.sleep(5)
    except KeyboardInterrupt as e:
        producer.close()
        print("Goodbye!")

