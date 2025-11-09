import json
import logging
import threading
from confluent_kafka import Consumer, KafkaError
from config import settings
from database import SessionLocal
from models import ObjectMetadata
from services.placement_engine import get_tier_for_object
from services.kafka_producer import kafka_producer
import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AccessConsumer:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': settings.kafka_bootstrap_servers,
            'group.id': 'access_event_group',
            'auto.offset.reset': 'earliest'
        })
        self.topic = settings.kafka_access_events_topic
        self.running = False

    def _process_message(self, msg):
        try:
            message_data = json.loads(msg.value().decode('utf-8'))
            object_id = message_data.get("object_id")

            if not object_id:
                logger.error(f"Invalid access event format: {message_data}")
                return

            db = SessionLocal()
            try:
                metadata = db.query(ObjectMetadata).filter(ObjectMetadata.object_id == object_id).first()
                if not metadata:
                    logger.warning(f"Received access event for non-existent object_id: {object_id}")
                    return

                # Update access metadata
                metadata.access_count += 1
                metadata.last_accessed_at = datetime.datetime.utcnow()
                
                logger.info(f"Object {object_id} accessed. New count: {metadata.access_count}")

                # Check for tier migration
                current_tier = metadata.current_tier
                new_tier = get_tier_for_object(metadata)

                if new_tier != current_tier:
                    logger.info(f"Tier change detected for {object_id}. From {current_tier} to {new_tier}.")
                    migration_message = {
                        "object_id": object_id,
                        "source_tier": current_tier,
                        "destination_tier": new_tier,
                        "reason": "reactive_access_spike"
                    }
                    kafka_producer.produce_message(
                        settings.kafka_migration_commands_topic,
                        key=object_id,
                        value=migration_message
                    )
                
                db.commit()
            finally:
                db.close()

        except json.JSONDecodeError:
            logger.error(f"Could not decode access event: {msg.value()}")
        except Exception as e:
            logger.error(f"Error processing access event: {e}")

    def start(self):
        self.running = True
        self.consumer.subscribe([self.topic])
        logger.info(f"Access consumer subscribed to topic '{self.topic}'.")
        
        while self.running:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(msg.error())
                continue
            
            self._process_message(msg)
        
        self.consumer.close()

def run_access_consumer():
    consumer = AccessConsumer()
    consumer.start()

def start_access_consumer_thread():
    thread = threading.Thread(target=run_access_consumer, daemon=True)
    thread.start()
    logger.info("Access event consumer thread started.")
    return thread
