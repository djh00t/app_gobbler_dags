import json
from confluent_kafka import Producer
from datetime import datetime, timedelta
import requests

# Import DAG and days_ago
from airflow import DAG
from airflow.utils.dates import days_ago

# Import Airflow Operators
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.http_operator import SimpleHttpOperator

# Import Airflow Hooks
from airflow.hooks.base_hook import BaseHook

# Set Variables
KAFKA_TOPIC = 'normalize'

# Set defaults for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['david@hooton.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_producer_config():
    try:
        conn = BaseHook.get_connection('kafka_producer_1')

        if not conn:
            raise ValueError("Connection 'kafka_producer_1' not found")

        extras = json.loads(conn.extra)

        # Debug Extras
        print(f"Producer config: {extras}")

        # Overwrite or add the bootstrap.servers field
        # extras['bootstrap.servers'] = f"{conn.host}:{conn.port}"

        return extras

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


# Process JSON object from 'get_klingon_serial', saving each key-value pair to
# XCom
def process_klingon_serial(ti):
    # Retrieve the response from XCom
    response_json = ti.xcom_pull(task_ids='get_klingon_serial', key='return_value')
    # Parse JSON response
    parsed_response = json.loads(response_json)
    # Push each key-value pair to XCom
    for key, value in parsed_response.items():
        ti.xcom_push(key=key, value=value)

# Extract the Klingon serial number from the JSON object
def pull_klingon_serial(ti):
    klingon_serial = ti.xcom_pull(task_ids='process_klingon_serial', key='serial')
    return klingon_serial

# Generate a Kafka message
def generate_kafka_message(ti):
    conf = get_producer_config()
    producer = Producer(conf)
    message_key_value = ti.xcom_pull(task_ids='process_klingon_serial', key='serial')
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
                    "nameOriginal": "s3://fsg-gobbler/recordings/raw/2023/07/[ Moiz]_2549-+61362705460_20230705035512(7873).wav"
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
with DAG(
    '00_04_test_generate_kafka_message_end_v01',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    description='DAG that generates normalize topic test messages',
    tags=["gobbler", "kafka", "normalize", "producer", "step_4"],
) as dag:

    # Get the JSON object from the API using the SimpleHttpOperator and
    # fission_router connection
    task_01_get_klingon_serial = SimpleHttpOperator(
        task_id='task_01_get_klingon_serial',
        method='GET',
        http_conn_id='fission_router',
        endpoint='/klingon-serial',
        dag=dag
    )

    # Process the JSON object from the API using the PythonOperator
    task_02_process_klingon_serial = PythonOperator(
        task_id='task_02_process_klingon_serial',
        python_callable=process_klingon_serial,
        provide_context=True,
        dag=dag,
    )

    # Pull the return_value key from the get_klingon_serial task_id in the same DAG
    # ID and Execution Date as this task then extract the value from the "serial"
    # key in the JSON object
    task_03_klingon_serial = PythonOperator(
        task_id='log_klingon_serial',
        python_callable=pull_klingon_serial,
        provide_context=True,
        dag=dag,
    )

    # Task 2 - Generate a Kafka message for the normalize topic
    t2 = PythonOperator(
        task_id='generate_kafka_message',
        python_callable=generate_kafka_message,
        dag=dag
        )
