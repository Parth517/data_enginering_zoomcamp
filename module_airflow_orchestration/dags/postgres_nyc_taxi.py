from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os
import requests
import gzip
import shutil
import hashlib
import pandas as pd

# ------------------------
# Config / Inputs
# ------------------------
TAXI_TYPE = "green"  # "yellow" or "green"
YEAR = "2019"
MONTH = "01"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

FILENAME = f"{TAXI_TYPE}_tripdata_{YEAR}-{MONTH}.csv"
STAGING_TABLE = f"{TAXI_TYPE}_tripdata_staging"
MAIN_TABLE = f"{TAXI_TYPE}_tripdata"

CSV_URL = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{TAXI_TYPE}/{FILENAME}.gz"

# ------------------------
# DAG Definition
# ------------------------
default_args = {
    'owner': 'zoomcamp',
    'start_date': datetime(2026, 1, 1),
    'retries': 1
}

dag = DAG(
    dag_id="04_postgres_taxi",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# ------------------------
# 1️⃣ Extract
# ------------------------
def extract():
    gz_path = os.path.join(DATA_DIR, FILENAME + ".gz")
    csv_path = os.path.join(DATA_DIR, FILENAME)

    # Download
    r = requests.get(CSV_URL, stream=True)
    r.raise_for_status()
    with open(gz_path, "wb") as f:
        f.write(r.content)

    # Unzip
    with gzip.open(gz_path, "rb") as f_in:
        with open(csv_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    os.remove(gz_path)
    print(f"Downloaded and extracted {FILENAME}")

t1_extract = PythonOperator(
    task_id="extract",
    python_callable=extract,
    dag=dag
)

# ------------------------
# 2️⃣ Load to Postgres
# ------------------------
def load_to_postgres():
    csv_path = os.path.join(DATA_DIR, FILENAME)
    df = pd.read_csv(csv_path)

    # Add unique_row_id & filename
    df['unique_row_id'] = df.apply(
        lambda row: hashlib.md5(
            (
                str(row.get('VendorID', '')) +
                str(row.get('tpep_pickup_datetime', row.get('lpep_pickup_datetime', ''))) +
                str(row.get('tpep_dropoff_datetime', row.get('lpep_dropoff_datetime', ''))) +
                str(row.get('PULocationID', '')) +
                str(row.get('DOLocationID', '')) +
                str(row.get('fare_amount', '')) +
                str(row.get('trip_distance', ''))
            ).encode('utf-8')
        ).hexdigest(),
        axis=1
    )
    df['filename'] = FILENAME

    # Connect to Postgres
    pg_hook = PostgresHook(postgres_conn_id="ny_taxi")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Create staging table
    columns_sql = ", ".join([f"{col} text" for col in df.columns])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {STAGING_TABLE} ({columns_sql});")
    conn.commit()

    # Truncate staging
    cursor.execute(f"TRUNCATE TABLE {STAGING_TABLE};")
    conn.commit()

    # Copy data to staging
    # Using pandas to insert row by row (simple approach)
    for i, row in df.iterrows():
        placeholders = ", ".join(["%s"] * len(row))
        columns = ", ".join(row.index)
        sql = f"INSERT INTO {STAGING_TABLE} ({columns}) VALUES ({placeholders})"
        cursor.execute(sql, tuple(row))
    conn.commit()

    # Merge into main table
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {MAIN_TABLE} ({columns_sql});")
    conn.commit()

    merge_sql = f"""
    INSERT INTO {MAIN_TABLE} ({', '.join(df.columns)})
    SELECT * FROM {STAGING_TABLE}
    ON CONFLICT (unique_row_id) DO NOTHING;
    """
    cursor.execute(merge_sql)
    conn.commit()
    cursor.close()
    conn.close()

t2_load = PythonOperator(
    task_id="load_to_postgres",
    python_callable=load_to_postgres,
    dag=dag
)

# ------------------------
# 3️⃣ Purge CSV (optional)
# ------------------------
def purge_csv():
    csv_path = os.path.join(DATA_DIR, FILENAME)
    if os.path.exists(csv_path):
        os.remove(csv_path)
        print(f"Deleted {FILENAME}")

t3_purge = PythonOperator(
    task_id="purge_csv",
    python_callable=purge_csv,
    dag=dag
)

# ------------------------
# Dependencies
# ------------------------
t1_extract >> t2_load >> t3_purge
