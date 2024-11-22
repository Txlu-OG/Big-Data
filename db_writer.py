from kafka import KafkaConsumer
from cockroach_connect import getConnection
import logging
from datetime import datetime
import re

# Setup logging
logging.basicConfig(level=logging.DEBUG)

# Get mandatory connection
conn = getConnection(True)

def sanitize_text(text):
    if text is None:
        return None
    # Remove non-printable characters except for standard whitespace
    return re.sub(r'[^\x20-\x7E]', '', text)

def setup_database():
    """Create the table if it does not exist."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS quickstart_events (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP,
                    value STRING
                )
            """)
            conn.commit()
            logging.debug("Table created or already exists.")
    except Exception as e:
        logging.error(f"Problem setting up the database: {e}")

def cockroachWrite(event):
    """Write Kafka event to the database."""
    try:
        # Read and convert data from event
        timestamp = int(event.timestamp / 1000)
        eventTimestamp = datetime.fromtimestamp(timestamp)
        eventValue = event.value.decode('utf-8', errors='replace')  # Decode with replacement

        # Sanitize eventValue to remove NUL bytes
        eventValue = sanitize_text(eventValue)

        # Insert into the database
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO quickstart_events (timestamp, value) 
                VALUES (%s, %s)
            """, (eventTimestamp, eventValue))
            conn.commit()
            logging.debug("Data inserted successfully.")
    except Exception as e:
        logging.error(f"Problem writing to database: {e}")

# Initialize Kafka consumer
consumer = KafkaConsumer(
    "connect-file-pulse-raw-logs-02",
    bootstrap_servers=['broker:29092'], 
    auto_offset_reset='earliest' # Replace with your Kafka server details
)

# Setup the database once
setup_database()

# Process messages
for msg in consumer:
    cockroachWrite(msg)
    logging.debug("Msg seen.") 
