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
    conf = {'bootstrap.servers': 'kafka.kafka:9092'}
    producer = Producer(conf)
    message_key_value = ti.xcom_pull(task_ids='process_klingon_serial', key='serial')
    message_key = {
        "taskID": message_key_value
    }
    message_headers = {
        'taskID': '167C267C606F0000118B5A20D253',
        'taskType': 'normalize',
        'taskEvents_step_01_datetime': '2023-05-16 13: 56: 03.172',
        'taskEvents_step_01_actor': "s3EventWatcher.fission@python-89822-99fb7dbb5-vcqhd['10.1.0.182']",
        'taskEvents_step_01_topic': 'normalize',
        'taskEvents_step_01_state': 'dispatched',
        'taskEvents_step_02_datetime': '2023-05-16 13: 56: 04.210',
        'taskEvents_step_02_actor': "normalizeConsumer.airflow@airflow-worker-0['10.1.1.115']",
        'taskEvents_step_02_state': 'queued',
        'taskEvents_step_03_datetime': '2023-05-16 13: 56: 05.844',
        'taskEvents_step_03_actor': "normalizeConsumer.airflow@airflow-worker-0['10.1.1.115']",
        'taskEvents_step_03_task': 'parse_file_name',
        'taskEvents_step_03_state': 'started',
        'taskEvents_step_04_datetime': '2023-05-16 13: 56: 08.333',
        'taskEvents_step_04_actor': "normalizeConsumer.airflow@airflow-worker-0['10.1.1.115']",
        'taskEvents_step_04_task': 'parse_file_name',
        'taskEvents_step_04_state': 'success',
        'taskEvents_step_05_datetime': '2023-05-16 13: 56: 11.965',
        'taskEvents_step_05_actor': "normalizeConsumer.airflow@airflow-worker-0['10.1.1.115']",
        'taskEvents_step_05_task': 'generate_filename',
        'taskEvents_step_05_state': 'started',
        'taskEvents_step_06_datetime': '2023-05-16 13: 56: 15.449',
        'taskEvents_step_06_actor': "normalizeConsumer.airflow@airflow-worker-0['10.1.1.115']",
        'taskEvents_step_06_task': 'generate_filename',
        'taskEvents_step_06_state': 'success',
        'taskEvents_step_07_datetime': '2023-05-16 13: 56: 18.424',
        'taskEvents_step_07_actor': "normalizeConsumer.airflow@airflow-worker-0['10.1.1.115']",
        'taskEvents_step_07_task': 'transcode_audio',
        'taskEvents_step_07_state': 'started',
        'taskEvents_step_08_datetime': '2023-05-16 13: 56: 23.372',
        'taskEvents_step_08_actor': "normalizeConsumer.airflow@airflow-worker-0['10.1.1.115']",
        'taskEvents_step_08_task': 'transcode_audio',
        'taskEvents_step_08_state': 'success',
        'taskEvents_step_09_datetime': '2023-05-16 13: 56: 27.183',
        'taskEvents_step_09_actor': "normalizeConsumer.airflow@airflow-worker-0['10.1.1.115']",
        'taskEvents_step_09_task': 'save_wav_to_s3',
        'taskEvents_step_09_state': 'started',
        'taskEvents_step_10_datetime': '2023-05-16 13: 56: 30.917',
        'taskEvents_step_10_actor': "normalizeConsumer.airflow@airflow-worker-0['10.1.1.115']",
        'taskEvents_step_10_task': 'save_wav_to_s3',
        'taskEvents_step_10_state': 'success',
        'taskEvents_step_11_datetime': '2023-05-16 13: 56: 33.102',
        'taskEvents_step_11_actor': "normalizeConsumer.airflow@airflow-worker-0['10.1.1.115']",
        'taskEvents_step_11_task': 'save_json_metadata_to_s3',
        'taskEvents_step_11_state': 'started',
        'taskEvents_step_12_datetime': '2023-05-16 13: 56: 34.609',
        'taskEvents_step_12_actor': "normalizeConsumer.airflow@airflow-worker-0['10.1.1.115']",
        'taskEvents_step_12_task': 'save_json_metadata_to_s3',
        'taskEvents_step_12_state': 'success',
        'taskEvents_step_13_datetime': '2023-05-16 13: 56: 38.533',
        'taskEvents_step_13_actor': "normalizeConsumer.airflow@airflow-worker-0['10.1.1.115']",
        'taskEvents_step_13_task': 'notify_controller_ok',
        'taskEvents_step_13_state': 'started',
        'taskEvents_step_14_datetime': '2023-05-16 13: 56: 41.156',
        'taskEvents_step_14_actor': "normalizeConsumer.airflow@airflow-worker-0['10.1.1.115']",
        'taskEvents_step_14_task': 'notify_controller_ok',
        'taskEvents_step_14_state': 'success'
    }
    message_value = {
        "tasks": {
            "normalize": {
                "file": {
                    "nameOriginal": "s3://fsg-gobbler/recordings/raw/2023/07/[ Moiz]_2549-+61362705460_20230705035512(7873).wav",
                    "nameNormalized": "s3://fsg-gobbler/recordings/normalized/2023/07/20230705_035512_7873_2549_61362705460.wav",
                    "agentName": "Moiz",
                    "agentXTN": "2549",
                    "callerID": "61362705460",
                    "date": "20230705",
                    "time": "035512",
                    "queueID": "7873"
                },
                "transcode": {
                    "transcoded": 1,
                    "formatOriginal": "wav",
                    "sampleRateOriginal": 8000,
                    "channelsOriginal": 1,
                    "bitDepthOriginal": 16,
                    "transcoded": 1,
                    "format": "wav",
                    "sampleRate": 16000,
                    "channels": 1,
                    "bitDepth": 16
                }
            }
        }
    }

    producer.produce(
        'normalize',
        key=json.dumps(message_key),
        headers = message_headers,
        value=json.dumps(message_value))

    producer.flush()

# Define the DAG
with DAG(
    'test_generate_kafka_message_v37',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    description='DAG that generates normalize topic test messages',
    tags=["gobbler", "kafka", "normalize", "rename", "transcode", "s3"],
) as dag:

    # Task 1 - Get the Klingon serial number using bash and jq
    t1 = BashOperator(
        task_id='get_serial',
        bash_command="curl -s http://router.fission/klingon-serial | jq -r '.serial'",
        dag=dag
        )

    # Task 2 - Generate a Kafka message for the normalize topic
    t2 = PythonOperator(
        task_id='generate_kafka_message',
        python_callable=generate_kafka_message,
        dag=dag
        )

    # Get the JSON object from the API
    http_task = SimpleHttpOperator(
        task_id='get_klingon_serial',
        method='GET',
        http_conn_id='fission_router',
        endpoint='/klingon-serial',
        dag=dag
    )

    # Add PythonOperator for the new task
    process_json_to_xcom = PythonOperator(
        task_id='process_klingon_serial',
        python_callable=process_klingon_serial,
        provide_context=True,
        dag=dag,
    )

    # Pull the return_value key from the get_klingon_serial task_id in the same DAG
    # ID and Execution Date as this task then extract the value from the "serial"
    # key in the JSON object
    log_task = PythonOperator(
        task_id='log_klingon_serial',
        python_callable=pull_klingon_serial,
        provide_context=True,
        dag=dag,
    )

# Set the upstream and downstream tasks
# Setting up the task dependencies
http_task >> process_json_to_xcom
t1 >> [log_task, t2]
process_json_to_xcom >> log_task
[http_task, t1] >> process_json_to_xcom
[log_task, t1] >> t2
