from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'djh00t',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='first_dag',
    default_args=default_args,
    description='My first DAG',
    start_date=datetime(2023, 10, 18, 3, 15),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo "Hello from task1"'
    )