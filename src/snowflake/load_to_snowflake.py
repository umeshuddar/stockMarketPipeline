# load_to_snowflake.py
import snowflake.connector
from minio import Minio
import io
import pandas as pd
from datetime import datetime

# MinIO configuration
minio_client = Minio(
    "minio:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Snowflake configuration
snowflake_conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account='YOUR_ACCOUNT',
    warehouse='COMPUTE_WH',
    database='STOCK_MARKET_DB',
    schema='PUBLIC'
)

def create_snowflake_table():
    cursor = snowflake_conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_data (
            ticker VARCHAR(10),
            timestamp TIMESTAMP,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume INTEGER,
            prev_close FLOAT,
            price_change FLOAT,
            price_change_pct FLOAT,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_summary (
            ticker VARCHAR(10),
            avg_close FLOAT,
            max_high FLOAT,
            min_low FLOAT,
            total_volume INTEGER,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    cursor.close()

def load_parquet_to_snowflake():
    date_str = datetime.now().strftime('%Y-%m-%d')
    bucket = "stock-market-data"

    # Load detailed data
    detailed_prefix = f"processed/{date_str}/detailed/"
    objects = list(minio_client.list_objects(bucket, prefix=detailed_prefix, recursive=True))

    cursor = snowflake_conn.cursor()
    for obj in objects:
        if obj.object_name.endswith('.parquet'):
            response = minio_client.get_object(bucket, obj.object_name)
            parquet_data = io.BytesIO(response.read())
            df = pd.read_parquet(parquet_data)

            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO stock_data (ticker, timestamp, open, high, low, close, volume, prev_close, price_change, price_change_pct)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row.get('ticker'), row.get('timestamp'),
                    row.get('open'), row.get('high'), row.get('low'), row.get('close'),
                    row.get('volume'), row.get('prev_close'),
                    row.get('price_change'), row.get('price_change_pct')
                ))
            print(f"Loaded {len(df)} records from {obj.object_name}")

    # Load summary data
    summary_prefix = f"processed/{date_str}/summary/"
    summary_objects = list(minio_client.list_objects(bucket, prefix=summary_prefix, recursive=True))

    for obj in summary_objects:
        if obj.object_name.endswith('.parquet'):
            response = minio_client.get_object(bucket, obj.object_name)
            parquet_data = io.BytesIO(response.read())
            df = pd.read_parquet(parquet_data)

            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO stock_summary (ticker, avg_close, max_high, min_low, total_volume)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    row.get('ticker'), row.get('avg_close'),
                    row.get('max_high'), row.get('min_low'), row.get('total_volume')
                ))
            print(f"Loaded {len(df)} summary records from {obj.object_name}")

    cursor.close()
    snowflake_conn.close()

create_snowflake_table()
load_parquet_to_snowflake()
print("Data successfully loaded to Snowflake!")
