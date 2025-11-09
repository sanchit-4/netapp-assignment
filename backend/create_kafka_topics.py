import logging
import time
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaError
from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_topics():
    """
    Connects to Kafka and creates the necessary topics for the application.
    Retries a few times to allow the Kafka broker to become available.
    """
    retries = 10
    while retries > 0:
        try:
            admin_client = AdminClient({'bootstrap.servers': settings.kafka_bootstrap_servers})
            
            topics = [
                NewTopic(topic=settings.kafka_ingest_topic, num_partitions=1, replication_factor=1),
                NewTopic(topic=settings.kafka_access_events_topic, num_partitions=1, replication_factor=1),
                NewTopic(topic=settings.kafka_migration_commands_topic, num_partitions=1, replication_factor=1)
            ]
            
            fs = admin_client.create_topics(topics)
            
            for topic, f in fs.items():
                try:
                    f.result(timeout=10)
                    logger.info(f"Topic '{topic}' created")
                except Exception as e:
                    kafka_error = e.args[0]
                    if kafka_error.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                        logger.warning(f"Topic '{topic}' already exists")
                    else:
                        logger.error(f"Failed to create topic '{topic}': {kafka_error}")
            
            # If we reach here, we have successfully connected and issued the command.
            return

        except Exception as e:
            retries -= 1
            logger.error(f"Failed to connect to Kafka AdminClient, retrying... ({retries} left): {e}")
            time.sleep(5)

if __name__ == "__main__":
    create_topics()
