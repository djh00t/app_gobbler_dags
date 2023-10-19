from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import requests
import json
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def get_serial():
    url = "http://fission.localdev.me/klingon-serial"

    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"An HTTP error occurred: {e}")
        raise

    try:
        data = response.json()
    except json.JSONDecodeError as e:
        print(f"A JSON decoding error occurred: {e}")
        raise

    serial = data.get('serial', None)

    if serial is None:
        print("The 'serial' key was not found in the JSON data")
        raise ValueError("The 'serial' key was not found in the JSON data")
    else:
        print(f"The serial value is: {serial}")

    # Save serial to an Airflow Variable
    Variable.set("run_serial", serial)

dag = DAG(
    'get_serial_dag',
    default_args=default_args,
    description='A simple DAG to get a serial value from a URL',
    schedule_interval='@daily',
    start_date=datetime(2023, 10, 19),
    catchup=False,
)

get_serial_task = PythonOperator(
    task_id='get_serial_task',
    python_callable=get_serial,
    dag=dag,
)
