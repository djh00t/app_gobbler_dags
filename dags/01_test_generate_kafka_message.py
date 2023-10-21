import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from confluent_kafka import Producer
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email': ['david@hooton.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_kafka_message():
    conf = {'bootstrap.servers': 'localhost:9092'}
    producer = Producer(conf)
    response = requests.get('http://router/klingon-serial')
    key = response.text
    message = {
        "header": {
            "subject": "normalize-file-name",
            "version": "1.0",
            "status": "new"
        },
        "body": {
            "file-name": "s3://fsg-gobbler/development/raw/2023/07/[Moiz]_2549-+61362705460_20230705035512(7873).wav",
            "last-action": "create",
            "next-action": "normalize-file-name"
        }
    }
    producer.produce('normalize', key=key, value=json.dumps(message))
    producer.flush()

dag = DAG('test_generate_kafka_message', default_args=default_args, schedule_interval=timedelta(1))

t1 = PythonOperator(
    task_id='generate_kafka_message',
    python_callable=generate_kafka_message,
    dag=dag)
