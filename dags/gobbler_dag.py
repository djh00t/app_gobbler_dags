from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import requests
import json
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def get_serial():
    url = "http://10.98.141.102/klingon-serial"

    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"An HTTP error occurred: {e}")
        raise

    try:
        data = response.json()
    except json.JSONDecodeError as e:
        print(f"A JSON decoding error occurred: {e}")
        raise

    serial = data.get('serial', None)

    if serial is None:
        print("The 'serial' key was not found in the JSON data")
        raise ValueError("The 'serial' key was not found in the JSON data")
    else:
        print(f"The serial value is: {serial}")

    # Save serial to an Airflow Variable
    Variable.set("run_serial", serial)

dag = DAG(
    'gobbler_normalize_file_dag_v01',
    default_args=default_args,
    description='Normalize File Name & Push to S3',
    schedule_interval='@daily',
    start_date=datetime(2023, 10, 19),
    catchup=False,
)

task01_get_workflow_serial = PythonOperator(
    task_id='get_serial_task',
    python_callable=get_serial,
    dag=dag,
)

task02_get_kafka_object = BashOperator(
    task_id='get_kafka_object_task',
    bash_command='echo "Extract Kafka Object"',
    dag=dag,
)

task03_get_s3_object = BashOperator(
    task_id='get_s3_object_task',
    bash_command='echo "Retrieve S3 Object"',
    dag=dag,
)

task04_normalize_file_name = BashOperator(
    task_id='normalize_file_name_task',
    bash_command='echo "Normalize File Name"',
    dag=dag,
)

task05_create_metadata_file = BashOperator(
    task_id='create_metadata_file_task',
    bash_command='echo "Create Metadata File"',
    dag=dag,
)

task06_validate_file_name = BashOperator(
    task_id='validate_file_name_task',
    bash_command='echo "Validate File Name Matches Schema"',
    dag=dag,
)

task07_validate_metadata_file = BashOperator(
    task_id='validate_metadata_file_task',
    bash_command='echo "Validate Metadata File Name & Content Matches Schema"',
    dag=dag,
)

task08_push_normalized_file_to_s3 = BashOperator(
    task_id='push_normalized_file_to_s3_task',
    bash_command='echo "Push Normalized File to S3"',
    dag=dag,
)

task09_push_metadata_file_to_s3 = BashOperator(
    task_id='push_metadata_file_to_s3_task',
    bash_command='echo "Push Metadata File to S3"',
    dag=dag,
)

task10_send_kafka_completion_message = BashOperator(
    task_id='send_kafka_completion_message_task',
    bash_command='echo "Send Kafka Completion Message"',
    dag=dag,
)

task01_get_workflow_serial >> task02_get_kafka_object >> task03_get_s3_object >> task04_normalize_file_name >> task05_create_metadata_file >> task06_validate_file_name >> task07_validate_metadata_file >> task08_push_normalized_file_to_s3 >> task09_push_metadata_file_to_s3 >> task10_send_kafka_completion_message
