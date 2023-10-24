from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, PythonSensor
from airflow.models import XCom
import re
from sqlalchemy import and_

# Set variables
VERSION='v1.0.0'

# Function to echo "GO TIME"
def echo_go_time(**kwargs):
    print("GO TIME")

# Function to check XCom for the triggering conditions
def check_trigger(**kwargs):
    session = kwargs['session']
    dag_id_pattern = "01_normalize_kafka_listener_%"
    results = session.query(XCom).filter(
        and_(
            XCom.dag_id.like(dag_id_pattern),
            or_(
                and_(XCom.key == 'taskID', XCom.value.op('SIMILAR TO')(r'([A-F0-9]{28})')),
                and_(XCom.key == 'goTime', XCom.value == b'OK')
            )
        )
    ).all()

    task_ids = [x.value.decode() for x in results if x.key == 'taskID']
    go_times = [x.value.decode() for x in results if x.key == 'goTime']

    return any(task_id in go_times for task_id in task_ids)

# Define default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
dag = DAG(
    '02_normalize_file_' + VERSION,
    default_args=default_args,
    description='An example DAG for file normalization',
    schedule_interval=None,  # Overridden at trigger time
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example'],
)

# Sensor task to wait for trigger
wait_for_trigger = PythonSensor(
    task_id='wait_for_trigger',
    python_callable=check_trigger,
    mode='poke',
    timeout=600,
    poke_interval=30,
    provide_context=True,
    dag=dag,
)

# Task to echo "GO TIME"
echo_go_time_task = PythonOperator(
    task_id='echo_go_time',
    python_callable=echo_go_time,
    dag=dag,
)

# Define task sequence
wait_for_trigger >> echo_go_time_task