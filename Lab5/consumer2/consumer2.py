from kafka import KafkaConsumer
import json
import time
from kafka.errors import NoBrokersAvailable

print('[Consumer2] Starting...')

def setup_consumer():
    try:
        consumer = KafkaConsumer(
            'Topic1',  # Для consumer2 змініть на 'Topic2'
            bootstrap_servers=['broker1:9092', 'broker2:9093'],
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='group2'  # Для consumer2 вкажіть 'group2'
        )
        return consumer
    except NoBrokersAvailable:
        print('[Consumer2] Brokers are not available. Retrying...')
        return None
    except Exception as e:
        print(f'[Consumer2] Error: {e}')
        return None

# Очікування доступності брокерів
consumer = None
while not consumer:
    try:
        consumer = setup_consumer()
        if not consumer:
            time.sleep(5)
    except KeyboardInterrupt:
        break

print('[Consumer2] Connected to Kafka. Waiting for messages...')

for message in consumer:
    print(f'[Consumer2] Received: {message.value}')