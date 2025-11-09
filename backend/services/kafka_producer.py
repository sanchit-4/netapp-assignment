import json
import logging
from confluent_kafka import Producer
from config import settings

logger = logging.getLogger(__name__)

class KafkaProducer:
    def __init__(self):
        self.producer = Producer({
            'bootstrap.servers': settings.kafka_bootstrap_servers
        })

    def produce_message(self, topic, key, value):
        try:
            self.producer.produce(topic, key=key, value=json.dumps(value))
            self.producer.flush(timeout=5)
            logger.info(f"Produced message to topic '{topic}': key={key}")
        except Exception as e:
            logger.error(f"Failed to produce message to topic '{topic}': {e}")

# Singleton instance
kafka_producer = KafkaProducer()
