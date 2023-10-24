# Import Airflow Modules
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook

# Import Kafka Modules
from confluent_kafka import Producer

# Import Other Modules
from datetime import datetime, timedelta
import json

# Set Variables
KAFKA_TOPIC = 'normalize'
KAFKA_CONNECTION = 'kafka_producer_1'

# Define the default arguments dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def get_producer_config():
    try:
        conn = BaseHook.get_connection( KAFKA_CONNECTION )
        if not conn:
            raise ValueError("Connection ", KAFKA_CONNECTION," not found")
        extras = json.loads(conn.extra)
        # Debug Extras
        print(f"Producer config: {extras}")
        return extras
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# Define the function to extract the serial value and save it to XCom
def extract_and_save_serial(**kwargs):
    # Get the HTTP response from XCom
    http_response = kwargs['ti'].xcom_pull(task_ids='task_01_get_klingon_serial')
    # Parse the JSON response
    response_json = json.loads(http_response)
    # Get the serial value
    serial_value = response_json.get('serial', '')
    # Push the serial value to XCom with the key 'taskID'
    kwargs['ti'].xcom_push(key='taskID', value=serial_value)

# Generate a Kafka message
def generate_kafka_message(ti):
    conf = get_producer_config()
    producer = Producer(conf)
    message_key_value = ti.xcom_pull(task_ids='task_02_extract_taskID', key='taskID')
    message_key = {
        "taskID": message_key_value
    }
    task_events = {
        "taskID": message_key_value,
        "taskType": "normalize",
        "taskEvents": {
            "step_1": {
            "datetime": "2023-05-16 13:56:03.172",
            "actor": "s3EventWatcher.fission@python-89822-99fb7dbb5-vcqhd['10.1.0.182']",
            "topic": "normalize",
            "state": "queued"
            }
        }
    }
    # Initialize message_headers as an empty dictionary
    message_headers = {}
    # Convert task_events to a string and add it to message_headers
    message_headers["taskEvents"] = json.dumps(task_events)
    # Convert message_headers to a list of tuples
    message_headers = list(message_headers.items())

    message_value = {
        "tasks": {
            "normalize": {
                "file": {
                    "nameOriginal": "s3://fsg-gobbler/recordings/raw/2023/07/[ John]_1234-+61355551234_20230705035512(5678).wav"
                }
            }
        }
    }

    producer.produce(
        KAFKA_TOPIC,
        key=json.dumps(message_key),
        headers = message_headers,
        value=json.dumps(message_value))

    producer.flush()




# Define the DAG
dag = DAG(
    '00_00_test_generate_kafka_message_start_v01',
    default_args=default_args,
    description='A simple DAG to send a GET request and process the response',
    schedule_interval='@daily',
    tags=["gobbler", "kafka", "normalize", "producer", "start", "step_00"],
)

# Get taskID from klingon-serial function
task_01_get_klingon_serial = SimpleHttpOperator(
    task_id='task_01_get_klingon_serial',
    method='GET',
    http_conn_id='fission_router',
    endpoint='/klingon-serial',
    dag=dag,
)

# Extract the taskID from the response and save it to XCom
task_02_extract_taskID = PythonOperator(
    task_id='task_02_extract_taskID',
    python_callable=extract_and_save_serial,
    provide_context=True,
    dag=dag,
)

# Generate a Kafka message for the normalize topic
task_03_generate_kafka_message = PythonOperator(
    task_id='task_03_generate_kafka_message',
    python_callable=generate_kafka_message,
    dag=dag
    )


# Set the task dependencies
task_01_get_klingon_serial >> task_02_extract_taskID >> task_03_generate_kafka_message
