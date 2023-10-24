# Import Airflow Modules
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import XCom
from airflow.utils.session import provide_session

# Import Kafka Modules
from confluent_kafka import Producer

# Import Other Modules
from datetime import datetime, timedelta
import json

# Set Variables
KAFKA_TOPIC = 'normalize'
KAFKA_CONNECTION = 'kafka_producer_1'

VERSION='v0.0.2'

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


# Example usage
# delete_xcom_variable(dag_id='my_dag', task_id='my_task', key='my_key')
@provide_session
def delete_xcom_variable(dag_id, task_id, key, session=None):
    """
    Delete an XCom variable based on dag_id, task_id, and key.

    :param dag_id: ID of the DAG that contains the XCom variable
    :param task_id: ID of the task that produced the XCom variable
    :param key: Key of the XCom variable
    :param session: SQLAlchemy ORM Session
    """
    xcom_entry = session.query(XCom).filter(
        XCom.dag_id == dag_id,
        XCom.task_id == task_id,
        XCom.key == key
    ).first()

    if xcom_entry:
        session.delete(xcom_entry)
        session.commit()
        print(f"Deleted XCom entry with key: {key}")
    else:
        print(f"No XCom entry found with key: {key}")

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
            "taskID": message_key_value,
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
    '00_00_test_generate_kafka_message_start_' + VERSION,
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

# Remove xcom variables that aren't needed
task_04_delete_xcom_variables = PythonOperator(
    task_id='task_04_delete_xcom_variables',
    python_callable=delete_xcom_variable,
    op_kwargs={'dag_id': dag.dag_id, 'task_id': 'task_01_get_klingon_serial', 'key': 'return_value'},
    provide_context=True,
    dag=dag
    )

# Set the task dependencies
task_01_get_klingon_serial >> task_02_extract_taskID >> [task_03_generate_kafka_message, task_04_delete_xcom_variables]
