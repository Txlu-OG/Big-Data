import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

# Set up logging
logging.basicConfig(level=logging.ERROR)

# Data to be sent to Kafka topics
customers = [
    {"customer_id": 1, "customer_name": "Tolu Ogun", "customer_email": "Tolu.o@gmail.com"},
    {"customer_id": 2, "customer_name": "Wale Thompson", "customer_email": "Wale.smtith@gmail.com"},
    {"customer_id": 3, "customer_name": "Seyi ola", "customer_email": "Seyi.o@gmail.com"},
    {"customer_id": 4, "customer_name": "Tireni ola", "customer_email": "Tireni.o@gmail.com"},
    {"customer_id": 5, "customer_name": "Tireni ola", "customer_email": "Tireni.o@gmail.com"},
]

book_sales = [
    {"sale_id": 1, "book_name": "Kafka way", "sale_amount": 45.50},
    {"sale_id": 2, "book_name": "Hard way", "sale_amount": 120.00},
    {"sale_id": 3, "book_name": "Data Science Way", "sale_amount": 75.00},
    {"sale_id": 4, "book_name": "The Pragmatic Programmer", "sale_amount": 50.00},
    {"sale_id": 5, "book_name": "High Performing Developers", "sale_amount": 110.00}
]

def produce_messages(topic, messages):
    producer = None
    try:
        # Initialize Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=['broker:29092', 'broker:39092', 'broker:49092'], 
            value_serializer=lambda m: json.dumps(m).encode('utf-8')
        )

        # Produce messages to the specified topic
        for message in messages:
            producer.send(topic, message)

    except KafkaError as e:
        logging.error("Error sending to Kafka broker: %s", e)
        exit(1)  # Exit with error code if Kafka error occurs

    finally:
        if producer is not None:
            producer.flush()
            producer.close()

# Produce customers data
produce_messages('customer-topic', customers)

# Produce book sales data
produce_messages('book-sales-topic', book_sales)

print("Messages produced successfully")