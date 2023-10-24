import json
import jsonschema
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow_provider_kafka.operators.await_message import AwaitKafkaMessageOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Set Kafka Variables
KAFKA_TOPIC = 'normalize'
KAFKA_CONNECTION = 'kafka_listener_1'
KAFKA_SCHEMA_KEY = '/opt/airflow/dags/repo/dags/kafka_schema_key.json'
KAFKA_SCHEMA_HEADER = '/opt/airflow/dags/repo/dags/kafka_schema_header.json'
KAFKA_SCHEMA_VALUE = '/opt/airflow/dags/repo/dags/kafka_schema_value.json'

# Load Kafka Message Schemas and log their successful loading
import logging

with open(KAFKA_SCHEMA_KEY, 'r') as file:
    schema_key = json.load(file)
print("[DEBUG] KAFKA_SCHEMA_KEY loaded successfully.")
print(f"[DEBUG] KAFKA_SCHEMA_KEY: {schema_key}")
logging.info("[DEBUG] KAFKA_SCHEMA_KEY loaded successfully.")

with open(KAFKA_SCHEMA_HEADER, 'r') as file:
    schema_header = json.load(file)
print("[DEBUG] KAFKA_SCHEMA_HEADER loaded successfully.")
print(f"[DEBUG] KAFKA_SCHEMA_HEADER: {schema_header}")
logging.info("[DEBUG] KAFKA_SCHEMA_HEADER loaded successfully.")

with open(KAFKA_SCHEMA_VALUE, 'r') as file:
    schema_value = json.load(file)
print("[DEBUG] KAFKA_SCHEMA_VALUE loaded successfully.")
print(f"[DEBUG] KAFKA_SCHEMA_VALUE: {schema_value}")
logging.info("[DEBUG] KAFKA_SCHEMA_VALUE loaded successfully.")

# Define the default arguments dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def get_kafka_config():
    try:
        conn = BaseHook.get_connection(KAFKA_CONNECTION)
        if not conn:
            print(f"[DEBUG] Connection {KAFKA_CONNECTION} not found")
            return None

        # Check if extras are available
        if not conn.extra:
            print(f"[DEBUG] No extras found for connection {KAFKA_CONNECTION}")
            return None

        # Load extras into a dictionary
        extras = json.loads(conn.extra)

        print(f"[DEBUG]  Kafka config: {extras}")

        # Debugging: Print the type and content of extras
        print(f"[DEBUG] Type of extras: {type(extras)}")
        print(f"[DEBUG] Content of extras: {extras}")

        return extras
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def hello_kafka():
    print("Hello Kafka !")
    return

# def validate_message(message):
#     # Add debugging
#     print(f"[DEBUG] Message: {message}")
#
#     # Validate the message key, headers, and value against the schemas
#     jsonschema.validate(instance=message['key'], schema=schema_key)
#     jsonschema.validate(instance=message['headers'], schema=schema_header)
#     jsonschema.validate(instance=message['value'], schema=schema_value)
#
#     return message

# def extract_task_id(message):
#     # Extract the taskID from the message key
#     task_id = message['key']['taskID']
#     return task_id, task_id
#
# def extract_file_name(message):
#     # Extract the file name from the message value
#     file_name = message['value']['tasks']['normalize']['file']['nameOriginal']
#     return 'file_name', file_name

dag = DAG(
        '01_kafka_triggered_normalize_v01.61d',
        default_args=default_args,
        description='Normalize Kafka Consumer DAG',
        tags=["gobbler", "kafka", "normalize", "consumer"]
    )

task_01_kafka_listener = AwaitKafkaMessageOperator(
    task_id='task_01_kafka_message_listen_validate',
    topics=[KAFKA_TOPIC],
    apply_function="hello_kafka",
    kafka_config=get_kafka_config(),
    xcom_push_key='retrieved_message',
    dag=dag,
)

task_01_kafka_listener.doc_md = "A deferable task. Reads the topic `KAFKA_TOPIC` until a message is encountered."



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
