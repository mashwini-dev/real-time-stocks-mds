import os
import sys
from dotenv import load_dotenv
import boto3
import snowflake.connector
from botocore.exceptions import ClientError
from airflow.models import Variable
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

load_dotenv()

def download_from_s3():
    s3_client = boto3.client("s3")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    s3_bucket= os.getenv("S3_BUCKET")
    s3_base_url= os.getenv("S3_BASE_URL")
    s3_region = os.getenv("AWS_REGION")
    local_dir = r"D:\ADF_folder\real-time-stocks-mds\data\s3_downloads" # "/tmp/s3_downloads"

    # stock_symbols = os.getenv("SYMBOLS")
    # symbols = Variable.get("SYMBOLS", deserialize_json=True)     # list of tickers (passed from DAG!)
    symbols = json.loads(os.environ.get("SYMBOLS", "[]"))

    print("in the function************************")
    os.makedirs(local_dir, exist_ok=True)

    s3 = boto3.client("s3", region_name=s3_region)

    all_downloaded_files = []

    for symbol in symbols:
        prefix = f"bronze/{symbol}/"
        print(f"\nProcessing prefix: {prefix}")

        response = s3.list_objects_v2(
            Bucket=s3_bucket,
            Prefix=prefix
        )

        objects = response.get("Contents", [])

        if not objects:
            print(f"No files found for {symbol}")
            continue

        for obj in objects:
            key = obj["Key"]

            if key.endswith("/"):
                continue

            local_file = os.path.join(
                local_dir,
                f"{symbol}_{os.path.basename(key)}"
            )

            s3.download_file(s3_bucket, key, local_file)
            print(f"Downloaded {key} -> {local_file}")

            all_downloaded_files.append(local_file)

    return all_downloaded_files
    
def load_to_snowflake(**kwargs):
    snowflake_user = os.getenv("SNOWFLAKE_USER")
    snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
    snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
    snowflake_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    snowflake_schema = os.getenv("SNOWFLAKE_SCHEMA")
    snowflake_db = os.getenv("SNOWKFLAKE_DATABASE")
    local_files = kwargs['ti'].xcom_pull(task_ids='download_s3')
    if not local_files:
        print("No files to load")
        return
    conn = snowflake.connector.connect(
        user = snowflake_user,
        password = snowflake_password,
        account = snowflake_account,
        warehouse = snowflake_warehouse,
        database = snowflake_db,
        schema = snowflake_schema
    )

    cur = conn.cursor()

    for f in local_files:
        cur.execute(f"PUT file://{f} @%bronze_stock_quotes_raw")
        print(f"Uploaded {f} to Snowflake stage")

    cur.execute("""
                COPY INTO bronze_stock_quotes_raw
                FROM @%bronze_stock_quotes_raw
                FILE_FORMAT =(TYPE=JSON)
                """)
    print("COPY INTO executed")
    cur.close()
    conn.close()

default_args ={
    "owner" :"airflow",
    "depends_on_past" : False,
    "start_date" :datetime(2025,2,23),
    "retries":1,
    "retry_delay":timedelta(minutes=5),
}

with DAG(
    "s3_to_snowflake",
    default_args=default_args,
    schedule=None,
    #schedule_interval="*/1 * * * *",
    catchup=False,
)as dag:
    task1 = PythonOperator(
        task_id = "download_s3",
        python_callable=download_from_s3,
    ) 
    task2 = PythonOperator(
        task_id = "load_snowflake",
        python_callable= load_to_snowflake,
        provide_context =True,
    )

    task1 >> task2