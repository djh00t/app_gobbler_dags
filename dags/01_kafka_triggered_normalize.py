import json
from airflow import DAG
from airflow.models import XCom
from confluent_kafka import Consumer, KafkaError
from airflow.models.baseoperator import BaseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Set Variables
KAFKA_TOPIC = 'normalize'
KAFKA_CONNECTION = 'kafka_listener_1'
VERSION='v01.7.0a'

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
                message_key = json.loads(msg.key()) if msg.key() else None
                message_headers = {k: json.loads(v) for k, v in dict(msg.headers()).items()} if msg.headers() else None

                context['task_instance'].xcom_push(key='message_value', value=message_value)
                context['task_instance'].xcom_push(key='message_key', value=message_key)
                context['task_instance'].xcom_push(key='message_headers', value=message_headers)

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

def hello_kafka():
    print("Hello Kafka !")
    return

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    "01_kafka_triggered_normalize_{VERSION}",
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

task_02_hello_kafka = PythonOperator(
    task_id='task_02_hello_kafka',
    python_callable=hello_kafka,
    dag=dag,
)
