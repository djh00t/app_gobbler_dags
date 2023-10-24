from datetime import datetime, timedelta
from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.models import XCom
from sqlalchemy import and_, or_
import json

# Set variables
VERSION='v1.0.0f'

# Function to echo "GO TIME"
def echo_go_time(**kwargs):
    print("GO TIME")

# Custom sensor to check XCom for the triggering conditions
class CustomXComSensor(BaseSensorOperator):
    def poke(self, context):
        session = settings.Session()
        dag_id = context['dag'].dag_id  # Get the dag_id from the context

        # Query for 'taskID'
        query_taskID = session.query(XCom).filter(
            and_(
                XCom.dag_id == dag_id,
                XCom.key == 'taskID'
            )
        ).first()

        # Query for 'goTime'
        query_goTime = session.query(XCom).filter(
            and_(
                XCom.dag_id == dag_id,
                XCom.key == 'goTime'
            )
        ).first()

        session.close()

        # Check if both 'taskID' and 'goTime' exist
        return query_taskID is not None and query_goTime is not None


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
