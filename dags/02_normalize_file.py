from datetime import datetime, timedelta
from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.models import XCom
from sqlalchemy import and_, or_
import re

# Set variables
VERSION='v1.0.0d'

# Function to echo "GO TIME"
def echo_go_time(**kwargs):
    print("GO TIME")

# Custom sensor to check XCom for the triggering conditions
class CustomXComSensor(BaseSensorOperator):

    def poke(self, context):
        session = settings.Session()
        dag_id_pattern = "01_normalize_kafka_listener_%"
        results = session.query(XCom).filter(
            XCom.dag_id.like(dag_id_pattern),
            or_(
                XCom.key == 'taskID',
                XCom.key == 'goTime'
            )
        ).all()

        task_ids = [x for x in results if x.key == 'taskID' and re.fullmatch(r'[A-F0-9]{28}', x.value)]
        go_times = [x for x in results if x.key == 'goTime' and x.value.decode() == 'OK']

        session.close()

        return any(task_id.dag_id == go_time.dag_id for task_id in task_ids for go_time in go_times)


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
    description='File normalization DAG',
    schedule_interval=None,  # Overridden at trigger time
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['normalize', 'file', 'gobbler'],
)

# Sensor task to wait for trigger
wait_for_trigger = CustomXComSensor(
    task_id='wait_for_trigger',
    mode='poke',
    timeout=600,
    poke_interval=30,
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
