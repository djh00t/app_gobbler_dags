import json
from airflow import DAG
from airflow.models import XCom
from confluent_kafka import Consumer, KafkaError
from airflow.models.baseoperator import BaseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.session import provide_session
from kubernetes import client, config

# Set Variables
KAFKA_TOPIC = 'normalize'
KAFKA_CONNECTION = 'kafka_listener_1'
VERSION='v1.0.1b'

def get_pod_ip():
    config.load_incluster_config()  # Use this if running within a cluster
    # config.load_kube_config()  # Use this if running locally
    v1 = client.CoreV1Api()
    pod_list = v1.list_namespaced_pod(namespace="airflow")

    for pod in pod_list.items:
        if pod.metadata.name.startswith('airflow-worker-0'):
            debug_print(f"Name: {pod.metadata.name}, IP: {pod.status.pod_ip}")
            break

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
        from airflow.hooks.base_hook import BaseHook
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

task_01_kafka_listener = KafkaConsumerOperator(
    task_id='task_01_kafka_listener',
    topics=[KAFKA_TOPIC],
    kafka_config=get_kafka_config(),
    dag=dag,
)

task_00_get_pod_ip = PythonOperator(
    task_id='task_00_get_pod_ip',
    python_callable=get_pod_ip,
    dag=dag,
)

task_00_get_pod_ip
task_01_kafka_listener
