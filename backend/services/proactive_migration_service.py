import logging
import threading
import time
from database import SessionLocal
from models import ObjectMetadata
from services.ml_service import ml_service
from services.placement_engine import get_tier_for_predicted_access
from services.kafka_producer import kafka_producer
from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProactiveMigrationService:
    def __init__(self, sweep_interval_seconds=60):
        self.running = False
        self.sweep_interval = sweep_interval_seconds

    def run_prediction_sweep(self):
        logger.info("Starting proactive migration sweep...")
        db = SessionLocal()
        try:
            objects = db.query(ObjectMetadata).all()
            logger.info(f"Found {len(objects)} objects to analyze.")
            
            for metadata in objects:
                try:
                    # Predict future access
                    predicted_access = ml_service.predict_access(metadata.object_id)
                    metadata.predicted_next_24h = predicted_access
                    
                    # Determine the tier based on the prediction
                    predicted_tier = get_tier_for_predicted_access(predicted_access)
                    current_tier = metadata.current_tier

                    if predicted_tier != current_tier:
                        logger.info(f"Proactive migration recommended for {metadata.object_id}. "
                                    f"Current: {current_tier}, Predicted: {predicted_tier}, "
                                    f"Predicted Access: {predicted_access:.2f}")
                        
                        # We only move "up" to a hotter tier proactively
                        if (current_tier == "public" and (predicted_tier == "private" or predicted_tier == "onprem")) or \
                           (current_tier == "private" and predicted_tier == "onprem"):
                            
                            migration_message = {
                                "object_id": metadata.object_id,
                                "source_tier": current_tier,
                                "destination_tier": predicted_tier,
                                "reason": "proactive_prediction"
                            }
                            kafka_producer.produce_message(
                                settings.kafka_migration_commands_topic,
                                key=metadata.object_id,
                                value=migration_message
                            )
                except Exception as e:
                    logger.error(f"Failed to process object {metadata.object_id} for proactive migration: {e}")

            db.commit()
        finally:
            db.close()
        logger.info("Proactive migration sweep finished.")

    def start(self):
        self.running = True
        logger.info(f"Proactive Migration Service started. Sweeping every {self.sweep_interval} seconds.")
        while self.running:
            self.run_prediction_sweep()
            time.sleep(self.sweep_interval)

def start_proactive_migration_thread():
    service = ProactiveMigrationService()
    thread = threading.Thread(target=service.start, daemon=True)
    thread.start()
    logger.info("Proactive migration service thread started.")
    return thread
