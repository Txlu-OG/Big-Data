import logging
from kafka import KafkaConsumer
import json
import threading

# Set up logging
logging.basicConfig(level=logging.INFO)

def consume_messages(topic):
    try:
        # Initialize Kafka consumer
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['broker:29092', 'broker:39092', 'broker:49092'],  # Ensure this matches the producer's bootstrap_servers
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        # Consume messages from the specified topic
        for message in consumer:
            print(f"Consumed message from {topic}: {message.value}")

    except Exception as e:
        logging.error("Error consuming messages from Kafka topic: %s", e)
        exit(1)  # Exit with error code if Kafka error occurs

if __name__ == '__main__':
    # Start threads to consume from both topics
    customer_thread = threading.Thread(target=consume_messages, args=('customer-topic',))
    book_sales_thread = threading.Thread(target=consume_messages, args=('book-sales-topic',))

    # Start the threads
    customer_thread.start()
    book_sales_thread.start()

    # Join the threads to the main thread
    customer_thread.join()
    book_sales_thread.join()
