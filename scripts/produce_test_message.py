import json
import uuid
from confluent_kafka import Producer
import sys
import os
import base64
import argparse

# Add backend to path to import settings
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from backend.config import settings

def produce_ingest_message(producer, topic, object_id, data_payload, is_file=False):
    """Produces a single message to the ingest topic."""
    message = {
        "object_id": object_id,
        "payload_data": data_payload,
        "is_file": is_file
    }
    producer.produce(topic, key=str(object_id), value=json.dumps(message))
    print(f"Produced message for object_id: {object_id}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Produce a test message to Kafka ingest topic.")
    parser.add_argument("--file", help="Path to a file whose content will be ingested.")
    args = parser.parse_args()

    # Kafka producer configuration
    producer_config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'ingest-producer'
    }

    # Create a producer instance
    producer = Producer(producer_config)
    
    # Topic to produce to
    ingest_topic = settings.kafka_ingest_topic

    sample_object_id = str(uuid.uuid4())
    payload_data = ""
    is_file = False

    if args.file:
        file_path = args.file
        if not os.path.exists(file_path):
            print(f"Error: File not found at {file_path}")
            sys.exit(1)
        with open(file_path, "rb") as f:
            payload_data = base64.b64encode(f.read()).decode('utf-8')
        is_file = True
        print(f"Ingesting content from file: {file_path}")
    else:
        payload_data = f"This is the content for object {sample_object_id}."
        print("Ingesting sample data.")

    # Produce and send the message
    produce_ingest_message(producer, ingest_topic, sample_object_id, payload_data, is_file)

    # Wait for any outstanding messages to be delivered and delivery reports received.
    producer.flush()
