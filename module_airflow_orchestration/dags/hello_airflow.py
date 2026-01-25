from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def hello_airflow():
    print("Hello, Airflow!")
    
with DAG(
    dag_id='hello_airflow_dag',
    start_date=datetime(2026, 1, 25),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=hello_airflow
    )
    
    hello_task