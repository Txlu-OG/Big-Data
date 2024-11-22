import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

logging.basicConfig(level=logging.ERROR)

try:
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )

    # Produce messages to topic 's1'
    for n in range(3):
        producer.send('s1', {'c1': str(n + 7), 'c2': n + 8})
    
except KafkaError as e:
    logging.error("Error sending to Kafka broker: %s", e)  # Corrected format here
    exit(1)  # Exit with error code if Kafka error occurs

finally:
    if producer is not None:
        producer.flush()
        producer.close()
