import logging
from confluent_kafka import Consumer, KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_consumer():
    conf = {
        'bootstrap.servers': 'kafka.kafka:9092',
        'group.id': 'group_1',
        'auto.offset.reset': 'earliest',
    }

    return Consumer(conf)

def kafka_listener(consumer, topic):
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # timeout in seconds; adjust as needed

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"{msg.topic()}:{msg.partition()} reached end at offset {msg.offset()}")
                elif msg.error():
                    raise KafkaError(msg.error())
            else:
                key = msg.key()
                value = msg.value()
                logger.info(f"Key: {key}, Value: {value}")

    except KeyboardInterrupt:
        pass
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def main():
    consumer = create_consumer()
    topic = 'normalize'
    kafka_listener(consumer, topic)

if __name__ == '__main__':
    main()
