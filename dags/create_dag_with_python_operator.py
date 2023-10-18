from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'djh00t',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

def greet(name, age):
    print(f"Hello World! My name is {name}, "
        f"and I am {age} years old.")

def get_name():
    return 'Jerry'

with DAG(
    default_args=default_args,
    dag_id='our_dag_with_python_operator_v03',
    description='Our DAG with Python Operator',
    start_date=datetime(2023, 10, 18, 5, 20),
    schedule_interval='@hourly'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={'name': 'djh00t', 'age': 30}
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=greet
    )

    task2
