from confluent_kafka import Producer
import yfinance as yf
import json
import time

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
}

# Create a Kafka producer
producer = Producer(conf)

# Define the Kafka topic
topic = 'stock-market-data'

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Fetch real-time stock data using yfinance
def fetch_stock_data(ticker):
    stock = yf.Ticker(ticker)
    hist = stock.history(period="1d", interval="1m")
    return hist

# Produce data to Kafka
def produce_stock_data(ticker):
    data = fetch_stock_data(ticker)
    for index, row in data.iterrows():
        record = {
            'ticker': ticker,
            'timestamp': str(index),
            'open': row['Open'],
            'high': row['High'],
            'low': row['Low'],
            'close': row['Close'],
            'volume': int(row['Volume'])
        }
        producer.produce(
            topic,
            key=ticker,
            value=json.dumps(record),
            callback=delivery_report
        )
        producer.poll(0)
    producer.flush()

# List of stock tickers to track
tickers = ['AAPL', 'GOOGL', 'MSFT', 'AMZN']

for ticker in tickers:
    produce_stock_data(ticker)
    print(f"Produced data for {ticker}")
