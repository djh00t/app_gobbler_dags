import json
from airflow import DAG
from airflow.providers.apache.kafka.operators.await_message import AwaitKafkaMessageOperator
from airflow.utils.dates import days_ago

KAFKA_TOPIC = 'normalize'
KAFKA_CONN_ID = 'kafka_listener'
KAFKA_SCHEMA_KEY = 'kafka_schema_key.json'
KAFKA_SCHEMA_HEADER = 'kafka_schema_header.json'
KAFKA_SCHEMA_VALUE = 'kafka_schema_value.json'

default_args = {
    'start_date': days_ago(1),
}

dag = DAG('kafka_listener', default_args=default_args)

def validate_message(message):
    # Validate that the message task and the topic match
    if message['taskID'] != dag.get_config('kafka_topic'):
        raise ValueError(f'Message taskID does not match topic: {message}')

    return True

def extract_task_id(message):
    # Extract the taskID from the message key
    task_id = message['key']['taskID']
    return task_id, task_id

def extract_file_name(message):
    # Extract the file name from the message value
    file_name = message['value']['tasks']['normalize']['file']['nameOriginal']
    return 'file_name', file_name

kafka_listener = AwaitKafkaMessageOperator(
    task_id='01_kafka_triggered_normalize_v01',
    topic=KAFKA_TOPIC,
    connection_id=KAFKA_CONN_ID,
    message_match_fn=validate_message,
    dag=dag,
)

task_id_xcom_pusher = PythonOperator(
    task_id='task_id_xcom_pusher',
    python_callable=extract_task_id,
    dag=dag,
)

file_name_xcom_pusher = PythonOperator(
    task_id='file_name_xcom_pusher',
    python_callable=extract_file_name,
    dag=dag,
)

kafka_listener >> task_id_xcom_pusher >> file_name_xcom_pusher
