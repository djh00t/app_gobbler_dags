from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"Hello World! My name is {first_name} {last_name}, "
          f"and I am {age} years old!")


def get_name(ti):
    ti.xcom_push(key='first_name', value='Jerry')
    ti.xcom_push(key='last_name', value='Fridman')

# Get Age function using ti (Task Instance) object
def get_age(ti):
    # Push age key/value for  to xcom
    ti.xcom_push(key='age', value=45)
##
## Example xcom record for our_dag_with_python_operator_v13 DAG ID
##
# Key           Value   Timestamp               Dag Id                              Task Id     Run Id                                  Map Index   Execution Date
# last_name     Fridman 2023-10-22, 23:00:48    our_dag_with_python_operator_v13    get_name    scheduled__2023-10-21T00:00:00+00:00                2023-10-21, 00:00:00
# first_name    Jerry   2023-10-22, 23:00:48    our_dag_with_python_operator_v13    get_name    scheduled__2023-10-21T00:00:00+00:00                2023-10-21, 00:00:00
# age           45      2023-10-22, 23:00:48    our_dag_with_python_operator_v13    get_age     scheduled__2023-10-21T00:00:00+00:00                2023-10-21, 00:00:00


with DAG(
    default_args=default_args,
    dag_id='our_dag_with_python_operator_v13',
    description='Create dag using python operator',
    start_date=datetime(2023, 10, 20),
    schedule_interval='@daily',
    tags=["gobbler", "xcom", "first_name", "last_name", "age"],
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )

    [task2, task3] >> task1
