from kafka import KafkaConsumer
from minio import Minio
import pandas as pd
import csv
import os
import time
import json

print('[Consumer2] Starting...')

# Налаштування MinIO
minio_client = Minio(
    "minio:9000",
    access_key="admin",
    secret_key="adminadmin!!",
    secure=False
)

# Перевірка з'єднання з MinIO
try:
    minio_client.list_buckets()
    print("[Consumer2] Підключено до MinIO")
except Exception as e:
    print(f"[Consumer2] Помилка підключення до MinIO: {e}")
    exit(1)

# Перевірка бакету
if not minio_client.bucket_exists("default"):
    print("Бакет 'default' не існує. Створюємо...")
    minio_client.make_bucket("default")

current_month_year = None
current_file = None
current_row_count = 0
MAX_ROWS_PER_FILE = 100  # Завантажувати кожні 100 рядків

def setup_consumer():
    try:
        consumer = KafkaConsumer(
            'Topic2',
            bootstrap_servers=['broker1:9092', 'broker2:9093'],
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='group2'
        )
        return consumer
    except Exception as e:
        print(f'[Consumer2] Помилка підключення до Kafka: {e}')
        return None

consumer = setup_consumer()
while not consumer:
    print('[Consumer2] Повторна спроба підключення...')
    time.sleep(5)
    consumer = setup_consumer()

print('[Consumer2] Підключено до Kafka. Обробка повідомлень...')

try:
    for message in consumer:
        try:
            data = message.value
            if 'start_time' not in data:
                print("[Consumer2] Поле 'start_time' відсутнє")
                continue

            date = pd.to_datetime(data['start_time'])
            month_year = date.strftime("%m_%Y")
            
            if month_year != current_month_year:
                if current_file:
                    try:
                        minio_client.fput_object("default", os.path.basename(current_file), current_file)
                        print(f"[Consumer2] Файл {current_file} завантажено (зміна місяця)")
                        os.remove(current_file)
                    except Exception as e:
                        print(f"[Consumer2] Помилка: {e}")

                current_month_year = month_year
                current_file = f"{current_month_year}.csv"
                current_row_count = 0
                with open(current_file, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(data.keys())

            # Запис даних
            with open(current_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(data.values())
                current_row_count += 1
                print(f"[Consumer2] Записано до {current_file}: {data}")

                # Завантажити частину файлу
                if current_row_count >= MAX_ROWS_PER_FILE:
                    try:
                        minio_client.fput_object("default", os.path.basename(current_file), current_file)
                        print(f"[Consumer2] Файл {current_file} завантажено (частина)")
                        current_row_count = 0
                    except Exception as e:
                        print(f"[Consumer2] Помилка: {e}")

        except Exception as e:
            print(f'[Consumer2] Помилка обробки: {e}')

except KeyboardInterrupt:
    print("[Consumer2] Зупинено користувачем")

finally:
    # Завантажити останній файл
    if current_file and os.path.exists(current_file):
        try:
            minio_client.fput_object("default", os.path.basename(current_file), current_file)
            print(f"[Consumer2] Фінальний файл {current_file} завантажено")
            os.remove(current_file)
        except Exception as e:
            print(f"[Consumer2] Помилка: {e}")