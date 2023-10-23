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
    'start_date': days_ago(2),
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
def extract_klingon_serial(response):
    json_object = json.loads(response.content)
    klingon_serial = json_object['serial']
    return klingon_serial

# Generate a Kafka message
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

# Define the DAG
dag = DAG(
    'test_generate_kafka_message_v17',
    default_args=default_args,
    schedule_interval=timedelta(1),
    tags=["gobbler", "kafka", "normalize-file-name"],
    )

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
    python_callable=extract_klingon_serial,

)

# Set the upstream and downstream tasks
# Setting up the task dependencies
http_task >> process_json_to_xcom
t1 >> [log_task, t2]
process_json_to_xcom >> log_task
[http_task, t1] >> process_json_to_xcom
[log_task, t1] >> t2
