import logging
from database import SessionLocal
from models import ObjectMetadata, MigrationEvent, TierEnum
from adapters.local_adapter import LocalStorageAdapter
from adapters.minio_adapter import MinioAdapter
from adapters.s3_adapter import S3Adapter

logger = logging.getLogger(__name__)

class MigrationService:
    def __init__(self):
        self.adapters = {
            "onprem": LocalStorageAdapter(),
            "private": MinioAdapter(),
            "public": S3Adapter()
        }

    def migrate_object(self, object_id: str, source_tier: str, destination_tier: str, reason: str):
        logger.info(f"Starting migration for {object_id} from {source_tier} to {destination_tier} (Reason: {reason})")
        
        source_adapter = self.adapters.get(source_tier)
        dest_adapter = self.adapters.get(destination_tier)

        if not all([source_adapter, dest_adapter]):
            logger.error(f"Invalid source or destination tier: {source_tier}, {destination_tier}")
            return

        db = SessionLocal()
        event = MigrationEvent(
            object_id=object_id,
            source_tier=source_tier,
            destination_tier=destination_tier,
            reason=reason,
            status="started"
        )
        db.add(event)
        db.commit()

        try:
            metadata = db.query(ObjectMetadata).filter(ObjectMetadata.object_id == object_id).first()
            if not metadata:
                raise ValueError(f"Cannot migrate non-existent object: {object_id}")

            # 1. Get object from source
            source_data = source_adapter.get(object_id)
            
            # 2. Put object into destination
            new_path, new_checksum = dest_adapter.put(object_id, source_data)

            # 3. Verify checksum
            # Note: ETag from S3 includes quotes, and can have a '-N' for multipart uploads.
            # A more robust solution would normalize these. For now, we strip quotes.
            if metadata.checksum != new_checksum.strip('"'):
                raise ValueError(f"Checksum mismatch for {object_id}. DB: {metadata.checksum}, New: {new_checksum}")

            # 4. Update metadata
            metadata.current_tier = destination_tier
            metadata.path = new_path
            metadata.version += 1
            
            event.status = "completed"
            db.commit()
            
            # 5. Delete from source
            source_adapter.delete(object_id)
            
            logger.info(f"Successfully migrated {object_id} to {destination_tier}")

        except Exception as e:
            logger.error(f"Error during migration for {object_id}: {e}")
            event.status = "failed"
            db.commit()
            # We don't rollback the event, so we have a record of the failure
        finally:
            db.close()

migration_service = MigrationService()
