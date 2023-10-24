from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.models import XCom
from sqlalchemy import and_, or_
import re

# Set variables
VERSION='v1.0.0o'
DEBUG = True

# Debugging function - only prints if DEBUG is set to True or 1
def debug_print(*args, **kwargs):
    # Check if the DEBUG environment variable is set to "true" or "1"
    if DEBUG in [True, 1]:
        print(*args, **kwargs)

# Example usage
debug_print("Debugging is ON.")

# Function to echo "GO TIME"
def echo_go_time(**kwargs):
    print("GO TIME")

# Custom sensor to check XCom for the triggering conditions
class CustomXComSensor(BaseSensorOperator):
    def poke(self, context):
        session = settings.Session()

        # Query for 'goTime' with specific dag_id pattern
        query_goTime = session.query(XCom).filter(
            and_(
                XCom.dag_id.like('01_normalize_kafka_listener_%'),
                XCom.key == 'goTime'
            )
        ).first()

        # Query for 'taskID' with specific dag_id pattern
        query_taskID = session.query(XCom).filter(
            and_(
                XCom.dag_id.like('01_normalize_kafka_listener_%'),
                XCom.key == 'taskID'
            )
        ).first()

        session.close()

        # Debug
        debug_print(f"query_goTime: {query_goTime}")
        debug_print(f"query_taskID: {query_taskID}")

        # Check if both 'taskID' and 'goTime' exist and additional conditions
        taskID_value = query_taskID.value if query_taskID else None
        goTime_value = query_goTime.value if query_goTime else None

        # Debugging
        debug_print(f"taskID_value: {taskID_value}, type: {type(taskID_value)}")
        debug_print(f"goTime_value: {goTime_value}, type: {type(goTime_value)}")

        return (taskID_value is not None and re.fullmatch(r'[a-fA-F0-9]{28}', taskID_value)) and \
               (goTime_value is not None and goTime_value == 'OK')
               # (goTime_value is not None and goTime_value.strip() == 'OK')  # Using strip() to remove any leading/trailing whitespaces



# Define default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
dag = DAG(
    '02_normalize_file_' + VERSION,
    default_args=default_args,
    description='File normalization DAG',
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['normalize', 'file', 'gobbler'],
)

# Task to echo "GO TIME"
echo_task = PythonOperator(
    task_id='echo_go_time',
    python_callable=echo_go_time,
    dag=dag,
)

# Sensor Task
sensor_task = CustomXComSensor(
    task_id='check_xcom',
    mode='poke',
    timeout=600,
    poke_interval=60,
    dag=dag,
)

# Define task sequence
sensor_task >> echo_task
