import json
from airflow import DAG
from airflow.models import XCom
from confluent_kafka import Consumer, KafkaError
from airflow.models.baseoperator import BaseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.session import provide_session
from kubernetes import client, config
from airflow.hooks.base_hook import BaseHook

# Set Variables
KAFKA_TOPIC = 'normalize'
KAFKA_CONNECTION = 'kafka_listener_1'
VERSION='v1.0.1e'
DEBUG=True
###
### Variables for headers
###
# Set pod name
pod='airflow-worker-0'
taskType='normalize'

# Debugging function - only prints if DEBUG is set to True or 1
def debug_print(*args, **kwargs):
    # Check if the DEBUG environment variable is set to "true" or "1"
    if DEBUG in [True, 1]:
        print(*args, **kwargs)

# Example usage
debug_print("Debugging is ON.")

def get_pod_ip(**kwargs):
    config.load_incluster_config()  # Use this if running within a cluster
    # config.load_kube_config()  # Use this if running locally
    v1 = client.CoreV1Api()
    pod_list = v1.list_namespaced_pod(namespace="airflow")

    for pod in pod_list.items:
        if pod.metadata.name.startswith('airflow-worker-0'):
            debug_print(f"Name: {pod.metadata.name}, IP: {pod.status.pod_ip}")
            # Set the pod IP to an XCom variable
            task_instance = kwargs['ti']
            task_instance.xcom_push(key='podIP', value=pod.status.pod_ip)
            break

# Add headers for this step to the message headers
def headers_generate(**kwargs):
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

    message_headers = task_instance.xcom_pull(task_ids='task_01_kafka_listener', key='message_headers')

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



# Kafka Consumer Operator
class KafkaConsumerOperator(BaseOperator):
    template_fields = ('topics',)

    def __init__(self, topics, kafka_config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.topics = topics
        self.kafka_config = kafka_config

    def execute(self, context):
        consumer = Consumer(self.kafka_config)
        consumer.subscribe(self.topics)

        try:
            while True:
                msg = consumer.poll(timeout=1.0)  # 1 second timeout, adjust as needed
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event - not an error
                        continue
                    else:
                        raise KafkaError(msg.error())
                # Process the message and save to XCom
                message_value = json.loads(msg.value()) if msg.value() else None
                message_key = json.loads(msg.key()).get('taskID') if msg.key() else None
                message_headers = {k: json.loads(v) for k, v in dict(msg.headers()).items()} if msg.headers() else None

                # Push extracted data to XCom
                context['task_instance'].xcom_push(key='message_value', value=message_value)
                context['task_instance'].xcom_push(key='taskID', value=message_key)
                context['task_instance'].xcom_push(key='message_headers', value=message_headers)

                # Push trigger value to XCom
                context['task_instance'].xcom_push(key='goTime', value='OK')

        except Exception as e:
            consumer.close()
            raise e

        consumer.close()

def get_kafka_config():
    try:
        conn = BaseHook.get_connection(KAFKA_CONNECTION)
        if not conn:
            print(f"[DEBUG] Connection {KAFKA_CONNECTION} not found")
            return None

        if not conn.extra:
            print(f"[DEBUG] No extras found for connection {KAFKA_CONNECTION}")
            return None

        extras = json.loads(conn.extra)
        print(f"[DEBUG]  Kafka config: {extras}")
        return extras
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# Delete xcom entry
# Example usage:
#   delete_xcom_variable(dag_id='my_dag', task_id='my_task', key='my_key')
@provide_session
def delete_xcom_variable(dag_id, task_id, key, session=None):
    """
    Delete an XCom variable based on dag_id, task_id, and key.

    :param dag_id: ID of the DAG that contains the XCom variable
    :param task_id: ID of the task that produced the XCom variable
    :param key: Key of the XCom variable
    :param session: SQLAlchemy ORM Session
    """
    xcom_entry = session.query(XCom).filter(
        XCom.dag_id == dag_id,
        XCom.task_id == task_id,
        XCom.key == key
    ).first()

    if xcom_entry:
        session.delete(xcom_entry)
        session.commit()
        print(f"Deleted XCom entry with key: {key}")
    else:
        print(f"No XCom entry found with key: {key}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    "01_normalize_kafka_listener_" + VERSION,
    default_args=default_args,
    description='Normalize Kafka Consumer DAG',
    tags=["gobbler", "kafka", "normalize", "consumer"]
)

task_00_get_pod_ip = PythonOperator(
    task_id='task_00_get_pod_ip',
    python_callable=get_pod_ip,
    provide_context=True,
    dag=dag,
)

task_01_kafka_listener = KafkaConsumerOperator(
    task_id='task_01_kafka_listener',
    topics=[KAFKA_TOPIC],
    kafka_config=get_kafka_config(),
    dag=dag,
)

task_02_headers_generate = PythonOperator(
    task_id='task_02_headers_generate',
    python_callable=headers_generate,
    provide_context=True,
    dag=dag,
)

task_00_get_pod_ip
task_01_kafka_listener
task_02_headers_generate
