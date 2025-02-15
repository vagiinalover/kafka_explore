from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092', 
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

while True:
    data = {"source": "producer2", "message": "Hello from producer 2"}
    producer.send("my_topic", value=data)
    print("Sent:", data)
    time.sleep(3)
