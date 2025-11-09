from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # MinIO Configuration
    minio_endpoint: str
    minio_access_key: str
    minio_root_password: str

    # AWS S3 Configuration
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_s3_bucket_name: str
    aws_region: str

    # Kafka Configuration
    kafka_bootstrap_servers: str
    kafka_ingest_topic: str
    kafka_access_events_topic: str
    kafka_migration_commands_topic: str

    # Database
    database_url: str

    # Tier Costs (per GB per month in USD)
    ONPREM_COST_PER_GB_MONTH: float = 0.01
    PRIVATE_COST_PER_GB_MONTH: float = 0.008
    PUBLIC_COST_PER_GB_MONTH: float = 0.005

    # Tier Latency (in ms)
    ONPREM_LATENCY_MS: float = 10.0
    PRIVATE_LATENCY_MS: float = 50.0
    PUBLIC_LATENCY_MS: float = 150.0

    class Config:
        env_file = ".env"

settings = Settings()
