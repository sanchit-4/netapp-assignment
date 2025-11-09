import logging
from fastapi import FastAPI, Depends, HTTPException
from contextlib import asynccontextmanager
from sqlalchemy.orm import Session

from database import engine, Base, get_db
import models
from services.ingestion_consumer import start_consumer_thread
from services.access_consumer import start_access_consumer_thread
from services.migration_consumer import start_migration_consumer_thread
from services.proactive_migration_service import start_proactive_migration_thread
from services.kafka_producer import kafka_producer
from services.placement_engine import get_tier_for_object
from services.ml_service import ml_service
from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# This will create the tables in the database
models.Base.metadata.create_all(bind=engine)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the Kafka consumers on startup
    start_consumer_thread()
    start_access_consumer_thread()
    start_migration_consumer_thread()
    start_proactive_migration_thread()
    yield
    # Threads are daemons and will exit with the main process

app = FastAPI(title="Data in Motion API", lifespan=lifespan)

@app.get("/")
def read_root():
    return {"message": "Welcome to the Data in Motion API"}

@app.post("/api/ml/train")
def train_ml_model():
    """
    Triggers the training of the ML model on a generated dummy dataset.
    """
    try:
        ml_service.train_model()
        return {"message": "ML model training initiated successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/objects/{object_id}/predict")
def predict_object_access(object_id: str):
    """
    Predicts the number of accesses for an object in the next 24 hours.
    """
    try:
        prediction = ml_service.predict_access(object_id)
        return {"object_id": object_id, "predicted_accesses_next_24h": prediction}
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Model not found. Please train the model first by calling /api/ml/train.")
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/objects/{object_id}/history")
def get_object_history(object_id: str, db: Session = Depends(get_db)):
    """
    Returns the metadata for an object, including predicted vs actual access.
    """
    metadata = db.query(models.ObjectMetadata).filter(models.ObjectMetadata.object_id == object_id).first()
    if not metadata:
        raise HTTPException(status_code=404, detail="Object not found")
    
    return {
        "object_id": metadata.object_id,
        "current_tier": metadata.current_tier,
        "access_count": metadata.access_count,
        "predicted_next_24h": metadata.predicted_next_24h,
        "last_accessed_at": metadata.last_accessed_at,
        "created_at": metadata.created_at,
    }

@app.post("/api/objects/{object_id}/access")
def access_object(object_id: str):
    """
    Simulates an access to an object, which may trigger a tier migration.
    """
    kafka_producer.produce_message(
        topic=settings.kafka_access_events_topic,
        key=object_id,
        value={"object_id": object_id}
    )
    return {"message": f"Access event for object {object_id} produced."}

@app.post("/api/objects/{object_id}/migrate")
def migrate_object_manually(object_id: str, db: Session = Depends(get_db)):
    """
    Manually triggers a re-evaluation of an object's tier placement.
    """
    metadata = db.query(models.ObjectMetadata).filter(models.ObjectMetadata.object_id == object_id).first()
    if not metadata:
        raise HTTPException(status_code=404, detail="Object not found")

    current_tier = metadata.current_tier
    new_tier = get_tier_for_object(metadata)

    if new_tier == current_tier:
        return {"message": f"Object {object_id} is already in the correct tier ({current_tier})."}

    logger.info(f"Manual migration trigger for {object_id}. From {current_tier} to {new_tier}.")
    migration_message = {
        "object_id": object_id,
        "source_tier": current_tier,
        "destination_tier": new_tier
    }
    kafka_producer.produce_message(
        settings.kafka_migration_commands_topic,
        key=object_id,
        value=migration_message
    )
    
    return {"message": f"Migration for object {object_id} from {current_tier} to {new_tier} has been queued."}

@app.get("/api/objects")
def get_all_objects(db: Session = Depends(get_db)):
    """
    Returns a list of all objects and their current metadata.
    """
    objects = db.query(models.ObjectMetadata).all()
    return [
        {
            "object_id": obj.object_id,
            "current_tier": obj.current_tier,
            "size_bytes": obj.size_bytes,
            "access_count": obj.access_count,
            "predicted_next_24h": obj.predicted_next_24h,
            "last_accessed_at": obj.last_accessed_at.isoformat() if obj.last_accessed_at else None,
            "created_at": obj.created_at.isoformat() if obj.created_at else None,
        }
        for obj in objects
    ]

@app.get("/api/metrics")
def get_system_metrics(db: Session = Depends(get_db)):
    """
    Returns system-wide metrics like tier distribution and estimated cost.
    """
    total_objects = db.query(models.ObjectMetadata).count()
    
    tier_distribution = {}
    estimated_monthly_cost = 0.0
    
    for tier_enum in models.TierEnum:
        tier = tier_enum.value
        objects_in_tier = db.query(models.ObjectMetadata).filter(models.ObjectMetadata.current_tier == tier).all()
        tier_distribution[tier] = len(objects_in_tier)
        
        for obj in objects_in_tier:
            cost_per_gb_month = getattr(settings, f"{tier.upper()}_COST_PER_GB_MONTH", 0.0)
            estimated_monthly_cost += (obj.size_bytes / (1024**3)) * cost_per_gb_month # Convert bytes to GB

    return {
        "total_objects": total_objects,
        "tier_distribution": tier_distribution,
        "estimated_monthly_cost": estimated_monthly_cost,
    }

@app.get("/api/migrations")
def get_migration_events(db: Session = Depends(get_db)):
    """
    Returns a list of recent migration events.
    """
    migrations = db.query(models.MigrationEvent).order_by(models.MigrationEvent.timestamp.desc()).limit(50).all()
    return [
        {
            "id": mig.id,
            "object_id": mig.object_id,
            "timestamp": mig.timestamp.isoformat(),
            "source_tier": mig.source_tier,
            "destination_tier": mig.destination_tier,
            "reason": mig.reason,
            "status": mig.status,
        }
        for mig in migrations
    ]
