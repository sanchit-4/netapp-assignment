import json
import threading
import logging
from confluent_kafka import Consumer, KafkaError
from config import settings
from database import SessionLocal
from adapters.local_adapter import LocalStorageAdapter
from adapters.minio_adapter import MinioAdapter
from adapters.s3_adapter import S3Adapter
import models
import io
import base64

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IngestionConsumer:
    def __init__(self):
        self.consumer_config = {
            'bootstrap.servers': settings.kafka_bootstrap_servers,
            'group.id': 'ingestion_group',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(self.consumer_config)
        self.topic = settings.kafka_ingest_topic
        self.running = False
        
        # Initialize adapters
        self.adapters = {
            "onprem": LocalStorageAdapter(),
            "private": MinioAdapter(),
            "public": S3Adapter()
        }

    def _process_message(self, msg):
        try:
            message_data = json.loads(msg.value().decode('utf-8'))
            object_id = message_data.get("object_id")
            payload_data_str = message_data.get("payload_data")
            is_file = message_data.get("is_file", False)

            if not all([object_id, payload_data_str]):
                logger.error(f"Invalid message format: {message_data}")
                return

            # For Stage 2, new objects are considered "cold" and go to the public tier.
            target_tier = "public"
            adapter = self.adapters[target_tier]
            
            if is_file:
                decoded_payload = base64.b64decode(payload_data_str)
            else:
                decoded_payload = payload_data_str.encode('utf-8')

            payload_size = len(decoded_payload)
            data_stream = io.BytesIO(decoded_payload)
            
            # Store the object
            storage_path, checksum = adapter.put(object_id, data_stream)
            
            # Save metadata to DB
            db = SessionLocal()
            try:
                metadata_record = models.ObjectMetadata(
                    object_id=object_id,
                    path=storage_path,
                    current_tier=target_tier,
                    size_bytes=payload_size,
                    checksum=checksum
                )
                db.add(metadata_record)
                db.commit()
                logger.info(f"Successfully ingested object {object_id} to {target_tier}")
            finally:
                db.close()

        except json.JSONDecodeError:
            logger.error(f"Could not decode message: {msg.value()}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def start(self):
        self.running = True
        self.consumer.subscribe([self.topic])
        logger.info(f"Kafka consumer subscribed to topic '{self.topic}' and starting.")
        
        while self.running:
            try:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(msg.error())
                        break
                
                self._process_message(msg)

            except Exception as e:
                logger.error(f"Unhandled exception in consumer loop: {e}")
        
        self.consumer.close()
        logger.info("Kafka consumer stopped.")

    def stop(self):
        self.running = False

def run_consumer():
    consumer = IngestionConsumer()
    try:
        consumer.start()
    except KeyboardInterrupt:
        consumer.stop()

def start_consumer_thread():
    consumer_thread = threading.Thread(target=run_consumer, daemon=True)
    consumer_thread.start()
    return consumer_thread
