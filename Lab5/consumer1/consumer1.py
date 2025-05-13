from kafka import KafkaConsumer
import json
import time
from kafka.errors import NoBrokersAvailable

print('[Consumer1] Starting...')

def setup_consumer():
    try:
        consumer = KafkaConsumer(
            'Topic1',  
            bootstrap_servers=['broker1:9092', 'broker2:9093'],
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='group1'  
        )
        return consumer
    except NoBrokersAvailable:
        print('[Consumer1] Brokers are not available. Retrying...')
        return None
    except Exception as e:
        print(f'[Consumer1] Error: {e}')
        return None

consumer = None
while not consumer:
    try:
        consumer = setup_consumer()
        if not consumer:
            time.sleep(5)
    except KeyboardInterrupt:
        break

print('[Consumer1] Connected to Kafka. Waiting for messages...')

for message in consumer:
    print(f'[Consumer1] Received: {message.value}')