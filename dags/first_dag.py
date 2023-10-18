from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'djh00t',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='first_dag_v4',
    default_args=default_args,
    description='My first DAG',
    start_date=datetime(2023, 10, 18, 5, 20),
    schedule_interval='@hourly'
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo "Hello from task1"'
    )
    task2 = BashOperator(
        task_id='task2',
        bash_command='echo "Hello from task2"'
    )
    task3 = BashOperator(
        task_id='task3',
        bash_command='echo "Hello from task3"'
    )
    task4 = BashOperator(
        task_id='task4',
        bash_command='echo "Hello from task4"'
    )
    task5 = BashOperator(
        task_id='task5',
        bash_command='echo "Hello from task5"'
    )
    task1.set_downstream(task2)
    task1.set_downstream(task3)
    task2.set_downstream(task4)
    task4.set_downstream(task5)