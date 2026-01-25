from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import json
import pandas as pd
import os

default_args = {
    'owner':'zoomcamp',
    'start_date': datetime(2026, 1, 25),
    'retries': 1
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '../data')
os.makedirs(DATA_DIR, exist_ok=True)

columms_to_keep = ["brand","price"]

def extract():
    url="https://dummyjson.com/products"
    response = requests.get(url)
    respone.raise_for_status()
    with open(os.path.join(DATA_DIR, 'data.json'), 'w') as f:
        f.write(response.text)
        
def transform():
    with open(os.path.join(DATA_DIR, 'data.json'), 'r') as f:
        data = json.load(f)
    filtered_data=[
        {col: product.get(col, "N/A") for col in columms_to_keep}
        for product in data['products']
    ]
    with open(os.path.join(DATA_DIR, 'products.json'), 'w') as f:
        json.dump(filtered_data, f)

def query():
    df=pd.read_json(os.path.join(DATA_DIR, 'products.json'))
    result= (
        df.groupby('brand')["price"]
        .mean()
        .reset_index()
        .sort_values(by='price', ascending=False)
    )
    print(result)
    
    result.to_json(os.path.join(DATA_DIR, 'avg_price.json'), orient='records', indent=4)
    
with DAG(
    dag_id='basic_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract
    )
    
    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform
    )
    
    query_task = PythonOperator(
        task_id='query_task',
        python_callable=query
    )
    
    extract_task >> transform_task >> query_task