import json
from confluent_kafka import Producer
from datetime import datetime, timedelta
import requests
from airflow import DAG
# Import Airflow Operators
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.http_operator import SimpleHttpOperator


# Set defaults for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['david@hooton.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

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
    'test_generate_kafka_message_v08',
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
    http_conn_id='klingon_api',
    endpoint='/klingon-serial',
    dag=dag
)

# Log the Klingon serial number
log_task = PythonOperator(
    task_id='log_klingon_serial',
    python_callable=extract_klingon_serial,
    op_kwargs={'response': '{{ task_instance.xcom_pull(task_id="get_klingon_serial") }}'},
    dag=dag
)

# Set the upstream and downstream tasks
http_task >> log_task >> t1 >> t2
