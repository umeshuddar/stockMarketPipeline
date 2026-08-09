from confluent_kafka import Consumer, KafkaException, KafkaError
import json
from minio import Minio
import io
import pandas as pd
from datetime import datetime

# Kafka Consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'realtime-stock-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['continuous-stock-data-producer'])

# MinIO Client
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket_name = "realtime-stock-data"
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

buffer = []
BATCH_SIZE = 10

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())

        record = json.loads(msg.value().decode('utf-8'))
        buffer.append(record)
        print(f"Consumed: {record['ticker']} at {record['close']}")

        if len(buffer) >= BATCH_SIZE:
            df = pd.DataFrame(buffer)
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer)
            parquet_buffer.seek(0)

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            object_name = f"realtime/stock_data_{timestamp}.parquet"

            minio_client.put_object(
                bucket_name,
                object_name,
                parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
            print(f"Uploaded {len(buffer)} records to MinIO: {object_name}")
            buffer = []

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
