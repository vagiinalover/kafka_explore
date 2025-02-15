from kafka import KafkaConsumer
import psycopg2
import json

# Connect to PostgreSQL
conn = psycopg2.connect("dbname=mydb user=myuser password=mypassword host=localhost port=5433")
cursor = conn.cursor()

# Create table
cursor.execute("""
CREATE TABLE IF NOT EXISTS kafka_data (
    id SERIAL PRIMARY KEY,
    source TEXT,
    message TEXT
)
""")
conn.commit()

# Consume from Kafka
consumer = KafkaConsumer("my_topic", 
                         bootstrap_servers='localhost:9092', 
                         auto_offset_reset='earliest', 
                         enable_auto_commit=True, 
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for msg in consumer:
    data = msg.value
    cursor.execute("INSERT INTO kafka_data (source, message) VALUES (%s, %s)", 
                   (data['source'], data['message']))
    conn.commit()
    print("Inserted into DB:", data)
