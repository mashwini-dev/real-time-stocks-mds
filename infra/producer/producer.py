import time
import json
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import sys

load_dotenv()

API_KEY = os.getenv("FINNHUB_API_KEY")

if not API_KEY:
    print("FINNHUB_API_KEY not found in environment variables")
    sys.exit(1)

BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL", "AMZN", "MSFT", "TSLA", "GOOGL"]

# If producer runs inside Docker network → use kafka:9092
# If running locally → use localhost:9092
#KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_BROKER = ["host.docker.internal:9092"]
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer= lambda v: json.dumps(v).encode("utf-8")
)


def fetch_quotes(symbol):
    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Finnhub returns empty dict if invalid symbol
        if not data or data.get("c") is None:
            print(f"No data received for {symbol}")
            return None

        data["symbol"] = symbol
        data["fetched_at"] = int(time.time())
        return data

    except requests.exceptions.RequestException as e:
        print(f"Error fetching {symbol}: {e}")
        return None


def main():
    print("Kafka producer started...")

    while True:
        for symbol in SYMBOLS:
            quote = fetch_quotes(symbol)
            if quote:
                print(f"Producing: {quote}")
                producer.send("stock-quotes", value=quote)
                producer.flush()   # Important inside Docker
            time.sleep(6)


if __name__ == "__main__":
    main()