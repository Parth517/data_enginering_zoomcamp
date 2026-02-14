"""@bruin

name: ingestion.trips
type: python
image: python:3.11

connection: duckdb-default   # ✅ ADD THIS


materialization:
  type: table
  strategy: append

columns:
  - name: vendorid
    type: integer
    description: Vendor ID

  - name: tpep_pickup_datetime
    type: timestamp
    description: Trip pickup timestamp

  - name: tpep_dropoff_datetime
    type: timestamp
    description: Trip dropoff timestamp

  - name: passenger_count
    type: integer
    description: Number of passengers

  - name: trip_distance
    type: double
    description: Distance traveled in miles

  - name: ratecodeid
    type: integer
    description: Rate code ID

  - name: store_and_fwd_flag
    type: string
    description: Store and forward flag

  - name: pulocationid
    type: integer
    description: Pickup location ID

  - name: dolocationid
    type: integer
    description: Dropoff location ID

  - name: payment_type
    type: integer
    description: Payment type ID

  - name: fare_amount
    type: double
    description: Fare amount

  - name: extra
    type: double
    description: Extra charges

  - name: mta_tax
    type: double
    description: MTA tax

  - name: tip_amount
    type: double
    description: Tip amount

  - name: tolls_amount
    type: double
    description: Tolls amount

  - name: improvement_surcharge
    type: double
    description: Improvement surcharge

  - name: total_amount
    type: double
    description: Total amount charged

  - name: congestion_surcharge
    type: double
    description: Congestion surcharge

  - name: airport_fee
    type: double
    description: Airport fee

  - name: taxi_type
    type: string
    description: Taxi type (yellow or green)

  - name: extracted_at
    type: timestamp
    description: Timestamp when record was extracted

@bruin"""

import os
import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"


def generate_months(start_date: str, end_date: str):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    current = start.replace(day=1)
    while current <= end:
        yield current.strftime("%Y-%m")
        current += relativedelta(months=1)


def materialize():
    # Read Bruin environment variables
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]

    vars_json = os.environ.get("BRUIN_VARS", "{}")
    vars_dict = json.loads(vars_json)

    taxi_types = vars_dict.get("taxi_types", ["yellow"])

    dataframes = []

    for taxi_type in taxi_types:
        for month in generate_months(start_date, end_date):
            file_name = f"{taxi_type}_tripdata_{month}.parquet"
            url = BASE_URL + file_name

            try:
                print(f"Fetching {url}")
                df = pd.read_parquet(url)

                # Add metadata columns
                df["taxi_type"] = taxi_type
                df["extracted_at"] = datetime.utcnow()

                # Standardize column names to lowercase
                df.columns = [col.lower() for col in df.columns]

                dataframes.append(df)

            except Exception as e:
                print(f"Skipping {url}: {e}")

    if not dataframes:
        return pd.DataFrame()

    final_df = pd.concat(dataframes, ignore_index=True)

    return final_df