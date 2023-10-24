from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.models import XCom, DagRun, TaskInstance
from airflow.utils.session import provide_session

from sqlalchemy import and_, or_
import re

# Set variables
VERSION='v1.0.0t'
DEBUG = True
KAFKA_HEADER_SCHEMA = '/opt/airflow/dags/repo/dags/kafka_schema_header.json'
KAFKA_VALUE_SCHEMA = '/opt/airflow/dags/repo/dags/kafka_schema_value.json'
###
### Variables for headers
###
# Set pod name
pod='airflow-worker-0'
taskType='normalize'
# Previous DAG and task to pull message headers from
headerDagId = '01_normalize_kafka_listener_%'
headerTaskId = 'task_01_kafka_listener'

def show_task_status(ti, **kwargs):
    dag_id = ti.dag_id
    execution_date = ti.execution_date

    print(f"Run ID: {ti.run_id}")
    print(f"Run duration: {ti.duration}")

    # Fetch DAG run and task instance info
    @provide_session
    def fetch_task_info(session):
        dag_run = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.execution_date == execution_date
        ).first()

        task_instances = session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.execution_date == execution_date
        ).all()

        if dag_run:
            print(f"Last scheduling decision: {dag_run.start_date}")
            print(f"Queued at: {dag_run.external_trigger}")
            print(f"Started: {dag_run.start_date}")
            print(f"Ended: {dag_run.end_date}")
            print(f"Data interval start: {dag_run.data_interval_start}")
            print(f"Data interval end: {dag_run.data_interval_end}")
            print(f"Externally triggered: {dag_run.external_trigger}")

        for ti in task_instances:
            print(f"Task ID: {ti.task_id}, State: {ti.state}, Start Time: {ti.start_date}, End Time: {ti.end_date}")

    fetch_task_info()

# Function to set the global taskID variable
taskID = None
def set_global_taskID(task_instance, **kwargs):
    global taskID
    message_headers = task_instance.xcom_pull(task_ids='previous_task_id', key='message_headers')
    message_values = task_instance.xcom_pull(task_ids='previous_task_id', key='message_values')
    taskID_headers = message_headers.get('taskEvents', {}).get('taskID', None)
    taskID_values = message_values.get('tasks', {}).get('taskID', None)

    if taskID_headers == taskID_values:
        taskID = taskID_headers
    else:
        raise ValueError("Task IDs in message_headers and message_values do not match")

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
        debug_print(f"taskID_value: {taskID_value}")

        # Set global taskID variable
        global taskID
        taskID = taskID_value

        # debug_print(f"taskID_value type: {type(taskID_value)}")
        debug_print(f"goTime_value: {goTime_value}")
        # debug_print(f"goTime_value type: {type(goTime_value)}")

        return (taskID_value is not None and re.fullmatch(r'[a-fA-F0-9]{28}', taskID_value)) and \
               (goTime_value is not None and goTime_value == 'OK')

# Add headers for this step to the message headers
def update_headers(**kwargs):
    task_instance = kwargs['ti']
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    pod_ip = task_instance.xcom_pull(task_ids='task_00_get_pod_ip')
    dag_run_status = task_instance.xcom_pull(task_ids='task_01_get_dag_run_status')
    now = task_instance.xcom_pull(task_ids='task_01_get_datetime')
    step_number = task_instance.xcom_pull(task_ids='task_01_get_next_step_number')
    debug_print(f"step_number is {step_number}")

    if step_number is None:
        step_number = 1
    else:
        step_number = int(step_number) + 1

    debug_print(f"step_number is {step_number}")

    # Pulling message_headers from a different DAG and task
    message_headers = task_instance.xcom_pull(task_ids=headerTaskId, dag_id=headerDagId, key='message_headers')

    debug_print(f"message_headers is {message_headers}")

    if message_headers is None:
        message_headers = {}

    if "taskEvents" not in message_headers:
        message_headers["taskEvents"] = {}

    message_headers["taskEvents"][f"step_{step_number}"] = {
        'datetime': now,
        'actor': f"{task_id}.{dag_id}@{pod}['{pod_ip}']",
        'task': task_id,
        'state': dag_run_status
    }

    debug_print(f"message_headers is {message_headers}")

    task_instance.xcom_push(key='message_headers', value=message_headers)

    return message_headers


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

# Sensor Task
task_01_check_xcom = CustomXComSensor(
    task_id='task_01_check_xcom',
    mode='poke',
    timeout=600,
    poke_interval=60,
    dag=dag,
)

task_02_set_taskID = PythonOperator(
    task_id='task_02_set_taskID',
    python_callable=set_global_taskID,
    provide_context=True,
)

task_03_update_headers = PythonOperator(
    task_id='task_03_update_headers',
    python_callable=update_headers,
    provide_context=True,
)

# Adding task to DAG
task_04_show_task_status = PythonOperator(
    task_id='task_04_show_task_status',
    python_callable=show_task_status,
    provide_context=True,
    dag=dag,
)

# Define task sequence
task_01_check_xcom >> task_02_set_taskID >> task_04_show_task_status
