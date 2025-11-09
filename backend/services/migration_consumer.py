import json
import logging
import threading
from confluent_kafka import Consumer, KafkaError
from config import settings
from services.migration_service import migration_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MigrationConsumer:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': settings.kafka_bootstrap_servers,
            'group.id': 'migration_group',
            'auto.offset.reset': 'earliest'
        })
        self.topic = settings.kafka_migration_commands_topic
        self.running = False

    def _process_message(self, msg):
        try:
            message_data = json.loads(msg.value().decode('utf-8'))
            object_id = message_data.get("object_id")
            source_tier = message_data.get("source_tier")
            destination_tier = message_data.get("destination_tier")
            reason = message_data.get("reason", "unknown") # Default reason

            if not all([object_id, source_tier, destination_tier]):
                logger.error(f"Invalid migration command format: {message_data}")
                return

            migration_service.migrate_object(object_id, source_tier, destination_tier, reason)

        except json.JSONDecodeError:
            logger.error(f"Could not decode migration command: {msg.value()}")
        except Exception as e:
            logger.error(f"Error processing migration command: {e}")

    def start(self):
        self.running = True
        self.consumer.subscribe([self.topic])
        logger.info(f"Migration consumer subscribed to topic '{self.topic}'.")
        
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

def run_migration_consumer():
    consumer = MigrationConsumer()
    consumer.start()

def start_migration_consumer_thread():
    thread = threading.Thread(target=run_migration_consumer, daemon=True)
    thread.start()
    logger.info("Migration command consumer thread started.")
    return thread
