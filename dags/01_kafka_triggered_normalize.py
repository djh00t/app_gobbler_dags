from datetime import datetime, timedelta
from confluent_kafka import Consumer, KafkaError
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook

# Topic name variable
KAFKA_TOPIC = 'normalize'

def get_consumer_config():
    conn = BaseHook.get_connection('kafka_listener')
    return {
        'bootstrap.servers': conn.host + ':' + str(conn.port),
        # 'group.id': conn.schema,
        'group.id': 'airflow_normalize_listener',
        'auto.offset.reset': 'beginning'
        # 'auto.offset.reset': 'earliest'
    }

consumer_config = get_consumer_config()

def kafka_listener_task(**kwargs):
    consumer = Consumer(consumer_config)
    consumer.subscribe([KAFKA_TOPIC])

    msg = consumer.poll(1.0)  # Poll for messages (timeout in seconds)

    if msg is None:
        print("No message received.")
        return

    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print("Reached end of partition.")
        else:
            print(msg.error())
        return

    message_key = msg.key().decode('utf-8')
    message_value = msg.value().decode('utf-8')

    # Validate that "task" and "topic" match
    task_name = kwargs['task'].task_id
    topic_name = msg.topic()
    if task_name != topic_name:
        print("Task and topic do not match.")
        return

    # Push taskID to XCom
    kwargs['ti'].xcom_push(key='taskID', value=message_key)

    # Extract file name from tasks.normalize.file.nameOriginal and push to XCom
    # Assuming message_value is a JSON-like string
    import json
    message_dict = json.loads(message_value)
    file_name = message_dict.get('tasks', {}).get('normalize', {}).get('file', {}).get('nameOriginal', '')
    if file_name:
        kwargs['ti'].xcom_push(key='file_name', value=file_name)

    consumer.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    '00_normalize_kafka_listener_dag_v03',
    default_args=default_args,
    description='An Airflow DAG to listen to the normalize Kafka topic',
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2023, 10, 22),
    catchup=False,
)

t1 = PythonOperator(
    task_id=KAFKA_TOPIC,
    python_callable=kafka_listener_task,
    provide_context=True,
    dag=dag,
)
