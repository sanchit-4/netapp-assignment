import datetime
from sqlalchemy import Column, String, Integer, DateTime, Float, Enum
from database import Base
import enum

class TierEnum(str, enum.Enum):
    onprem = "onprem"
    private = "private"
    public = "public"

class ObjectMetadata(Base):
    __tablename__ = "object_metadata"

    object_id = Column(String, primary_key=True, index=True)
    path = Column(String, nullable=False)
    current_tier = Column(Enum(TierEnum), nullable=False)
    size_bytes = Column(Integer)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    last_accessed_at = Column(DateTime, default=datetime.datetime.utcnow)
    access_count = Column(Integer, default=0)
    version = Column(Integer, default=1)
    checksum = Column(String)
    predicted_next_24h = Column(Float)
    cost_per_gb_month = Column(Float)
    latency_ms_est = Column(Float)

class MigrationEvent(Base):
    __tablename__ = "migration_events"
    
    id = Column(Integer, primary_key=True, index=True)
    object_id = Column(String, index=True)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    source_tier = Column(Enum(TierEnum))
    destination_tier = Column(Enum(TierEnum))
    reason = Column(String) # e.g., "proactive", "reactive"
    status = Column(String) # e.g., "started", "completed", "failed"
