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
topic = 'continuous-stock-data-producer'

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Fetch real-time stock data using yfinance
def fetch_stock_data(ticker):
    stock = yf.Ticker(ticker)
    data = stock.history(period="1d", interval="1m")
    if not data.empty:
        latest = data.iloc[-1]
        record = {
            'ticker': ticker,
            'timestamp': str(data.index[-1]),
            'open': latest['Open'],
            'high': latest['High'],
            'low': latest['Low'],
            'close': latest['Close'],
            'volume': int(latest['Volume'])
        }
        return record
    return None

# Continuously produce data to Kafka
def continuous_producer(tickers, interval=60):
    print(f"Starting continuous producer for {tickers} with {interval}s interval")
    while True:
        for ticker in tickers:
            record = fetch_stock_data(ticker)
            if record:
                producer.produce(
                    topic,
                    key=ticker,
                    value=json.dumps(record),
                    callback=delivery_report
                )
                producer.poll(0)
                print(f"Produced data for {ticker}: {record['close']}")
        producer.flush()
        print(f"Waiting {interval} seconds...")
        time.sleep(interval)

tickers = ['AAPL', 'GOOGL', 'MSFT', 'AMZN']
continuous_producer(tickers, interval=60)
