import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from confluent_kafka import Producer
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 20),
    'email': ['david@hooton.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'tags': ['gobbler', 'kafka', 'normalize-file-name'],
}

def generate_kafka_message():
    conf = {'bootstrap.servers': 'kafka.kafka:9092'}
    producer = Producer(conf)
    response = requests.get('http://router.fission/klingon-serial')
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
    producer.produce(
        'normalize',
        key=key,
        value=json.dumps(message))

    producer.flush()

dag = DAG(
    'test_generate_kafka_message_v05',
    default_args=default_args,
    schedule_interval=timedelta(1)
    )

t1 = BashOperator(
    task_id='get_serial',
    bash_command='curl -s http://router.fission/klingon-serial',
    dag=dag
    )

t2 = PythonOperator(
    task_id='generate_kafka_message',
    python_callable=generate_kafka_message,
    dag=dag
    )

t1 >> t2
