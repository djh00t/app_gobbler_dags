import requests
from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
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

# The generate_kafka_message function has been moved to a separate Python script.

dag = DAG('test_generate_kafka_message', default_args=default_args, schedule_interval=timedelta(1))

t1 = DockerOperator(
    task_id='generate_kafka_message',
    image='djh00t/gobbler-airflow',
    command='python /path/to/generate_kafka_message.py',
    dag=dag)
