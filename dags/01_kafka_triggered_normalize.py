import json
import jsonschema
from airflow import DAG
from airflow_provider_kafka.operators.await_message import AwaitKafkaMessageOperator
from airflow.utils.dates import days_ago


KAFKA_TOPIC = 'normalize'
KAFKA_CONN_ID = 'kafka_listener'
KAFKA_SCHEMA_KEY = '/opt/airflow/dags/repo/dags/kafka_schema_key.json'
KAFKA_SCHEMA_HEADER = '/opt/airflow/dags/repo/dags/kafka_schema_header.json'
KAFKA_SCHEMA_VALUE = '/opt/airflow/dags/repo/dags/kafka_schema_value.json'

with open(KAFKA_SCHEMA_KEY, 'r') as file:
    schema_key = json.load(file)

with open(KAFKA_SCHEMA_HEADER, 'r') as file:
    schema_header = json.load(file)

with open(KAFKA_SCHEMA_VALUE, 'r') as file:
    schema_value = json.load(file)

default_args = {
    'start_date': days_ago(1),
}

def validate_message(message):
    # Validate that the message task and the topic match
    if message['taskID'] != dag.get_config('kafka_topic'):
        raise ValueError(f'Message taskID does not match topic: {message}')

    # Validate the message key, headers, and value against the schemas
    jsonschema.validate(instance=message['key'], schema=schema_key)
    jsonschema.validate(instance=message['headers'], schema=schema_header)
    jsonschema.validate(instance=message['value'], schema=schema_value)

    return message

def extract_task_id(message):
    # Extract the taskID from the message key
    task_id = message['key']['taskID']
    return task_id, task_id

def extract_file_name(message):
    # Extract the file name from the message value
    file_name = message['value']['tasks']['normalize']['file']['nameOriginal']
    return 'file_name', file_name

dag = DAG(
        '01_kafka_triggered_normalize_v01',
        default_args=default_args,
        description='Normalize Kafka Consumer DAG',
        tags=["gobbler", "kafka", "normalize", "consumer"]
    )

task_01_kafka_listener = AwaitKafkaMessageOperator(
    task_id='task_01_kafka_message_listen_validate',
    topics=[KAFKA_TOPIC],
    connection_id=KAFKA_CONN_ID,
    apply_function=validate_message,
    do_xcom_push=True,
    dag=dag,
)

# task_02_task_id_xcom_pusher = PythonOperator(
#     task_id='task_02_task_id_xcom_pusher',
#     python_callable=extract_task_id,
#     dag=dag,
# )
#
# task_03_file_name_xcom_pusher = PythonOperator(
#     task_id='task_03_file_name_xcom_pusher',
#     python_callable=extract_file_name,
#     dag=dag,
# )

task_01_kafka_listener
# task_01_kafka_listener >> task_02_task_id_xcom_pusher >> task_03_file_name_xcom_pusher
