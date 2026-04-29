import json
import boto3
from dotenv import load_dotenv
import os
import sys
import time
from kafka import KafkaConsumer
from datetime import datetime, timezone


load_dotenv()

s3_base_url= os.getenv("S3_BASE_URL")
bucket = os.getenv("S3_BUCKET")

today = datetime.now(timezone.utc)
s3_client = boto3.client("s3")

consumer = KafkaConsumer(
    "stock-quotes",
    #bootstrap_servers=["host.docker.internal:29092"],
    bootstrap_servers=["localhost:29092"],
    enable_auto_commit= True,
    auto_offset_reset ="earliest",
    group_id="bronze-consumer",
    value_deserializer= lambda v:json.loads(v.decode("utf-8"))
)

print("Consumer streaming and saving to S3...")
for message in consumer:
    record = message.value
    symbol=record.get("symbol")
    print("s3=",bucket,"==symbol--",symbol)
    ts = record.get("fetched_at",int(time.time()))
    key = f"bronze/{symbol}/{ts}.json"

    s3_client.put_object(
    Bucket=bucket,
    Key=key,
    Body=json.dumps(record),
    ContentType="application/json"
    )
    print(f"Raw data uploaded to s3://{bucket}/{key}")