from kafka import KafkaProducer
import pandas as pd
import json
import time
from kafka.errors import NoBrokersAvailable

print('[Producer] Starting...')

def setup_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=['broker1:9092', 'broker2:9093'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        return producer
    except NoBrokersAvailable:
        print('[Producer] Brokers are not available. Retrying...')
        return None
    except Exception as e:
        print(f'[Producer] Error: {e}')
        return None

# Очікування доступності брокерів
producer = None
while not producer:
    try:
        producer = setup_producer()
        if not producer:
            time.sleep(5)
    except KeyboardInterrupt:
        break

print('[Producer] Connected to Kafka. Sending messages...')

# Читання CSV
df = pd.read_csv('/app/data/Divvy_Trips_2019_Q4.csv')
for _, row in df.iterrows():
    data = row.to_dict()
    producer.send('Topic1', data)
    producer.send('Topic2', data)
    print(f'[Producer] Sent: {data}')
    time.sleep(0.1)  # Затримка

producer.flush()