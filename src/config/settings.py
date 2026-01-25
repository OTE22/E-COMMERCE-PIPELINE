"""
Real-Time E-Commerce Analytics Platform
Centralized Configuration Management

This module provides production-grade configuration management using Pydantic
settings with environment variable support, validation, and type safety.
"""

from functools import lru_cache
from typing import Optional, List
from pydantic import Field, field_validator, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """PostgreSQL Database Configuration"""
    
    model_config = SettingsConfigDict(env_prefix="POSTGRES_")
    
    host: str = Field(default="localhost", description="Database host")
    port: int = Field(default=5432, description="Database port")
    db: str = Field(default="ecommerce_analytics", alias="database", description="Database name")
    user: str = Field(default="ecommerce", description="Database user")
    password: SecretStr = Field(default="secure_password", description="Database password")
    pool_size: int = Field(default=20, description="Connection pool size")
    max_overflow: int = Field(default=10, description="Max overflow connections")
    pool_timeout: int = Field(default=30, description="Pool timeout in seconds")
    echo: bool = Field(default=False, description="Echo SQL queries")
    
    @property
    def async_url(self) -> str:
        """Async database URL for asyncpg"""
        return f"postgresql+asyncpg://{self.user}:{self.password.get_secret_value()}@{self.host}:{self.port}/{self.db}"
    
    @property
    def sync_url(self) -> str:
        """Sync database URL for psycopg2"""
        return f"postgresql+psycopg2://{self.user}:{self.password.get_secret_value()}@{self.host}:{self.port}/{self.db}"


class RedisSettings(BaseSettings):
    """Redis Cache Configuration"""
    
    model_config = SettingsConfigDict(env_prefix="REDIS_")
    
    host: str = Field(default="localhost", description="Redis host")
    port: int = Field(default=6379, description="Redis port")
    password: Optional[SecretStr] = Field(default=None, description="Redis password")
    db: int = Field(default=0, description="Redis database number")
    max_connections: int = Field(default=100, description="Max connections")
    socket_timeout: int = Field(default=5, description="Socket timeout in seconds")
    decode_responses: bool = Field(default=True, description="Decode responses to strings")
    url: Optional[str] = Field(default=None, alias="REDIS_URL", description="Redis URL (overrides host/port)")
    
    def get_url(self) -> str:
        """Redis connection URL - uses REDIS_URL if set, otherwise builds from host/port"""
        if self.url:
            return self.url
        if self.password:
            return f"redis://:{self.password.get_secret_value()}@{self.host}:{self.port}/{self.db}"
        return f"redis://{self.host}:{self.port}/{self.db}"


class KafkaSettings(BaseSettings):
    """Kafka Streaming Configuration"""
    
    model_config = SettingsConfigDict(env_prefix="KAFKA_")
    
    bootstrap_servers: str = Field(default="localhost:9092", description="Kafka bootstrap servers")
    consumer_group: str = Field(default="ecommerce-analytics", description="Consumer group ID")
    auto_offset_reset: str = Field(default="earliest", description="Auto offset reset policy")
    enable_auto_commit: bool = Field(default=True, description="Enable auto commit")
    max_poll_records: int = Field(default=500, description="Max poll records")
    session_timeout_ms: int = Field(default=30000, description="Session timeout")
    heartbeat_interval_ms: int = Field(default=10000, description="Heartbeat interval")
    
    # Topic configuration
    topics_orders: str = Field(default="orders", description="Orders topic")
    topics_clickstream: str = Field(default="clickstream", description="Clickstream topic")
    topics_events: str = Field(default="events", description="Events topic")
    topics_anomalies: str = Field(default="anomalies", description="Anomalies topic")
    
    @property
    def topics(self) -> List[str]:
        """List of all configured topics"""
        return [
            self.topics_orders,
            self.topics_clickstream,
            self.topics_events,
            self.topics_anomalies,
        ]


class DataLakeSettings(BaseSettings):
    """Data Lake Storage Configuration"""
    
    model_config = SettingsConfigDict(env_prefix="DATA_")
    
    lake_path: str = Field(default="./data", description="Data lake root path")
    raw_path: str = Field(default="./data/raw", description="Raw data zone path")
    staging_path: str = Field(default="./data/staging", description="Staging zone path")
    curated_path: str = Field(default="./data/curated", description="Curated zone path")
    
    # File formats
    default_format: str = Field(default="parquet", description="Default file format")
    compression: str = Field(default="snappy", description="Compression codec")
    partition_by: List[str] = Field(default=["year", "month", "day"], description="Partition columns")


class SecuritySettings(BaseSettings):
    """Security and Authentication Configuration"""
    
    model_config = SettingsConfigDict(env_prefix="")
    
    secret_key: SecretStr = Field(default="change-me-in-production", description="Application secret key")
    jwt_secret_key: SecretStr = Field(default="jwt-secret-change-me", alias="JWT_SECRET_KEY", description="JWT secret key")
    jwt_algorithm: str = Field(default="HS256", alias="JWT_ALGORITHM", description="JWT algorithm")
    jwt_expiration_hours: int = Field(default=24, alias="JWT_EXPIRATION_HOURS", description="JWT expiration in hours")
    encryption_key: SecretStr = Field(default="32-byte-key-placeholder", alias="ENCRYPTION_KEY", description="Data encryption key")
    
    # Rate limiting
    rate_limit_requests: int = Field(default=100, alias="RATE_LIMIT_REQUESTS", description="Rate limit requests")
    rate_limit_window_seconds: int = Field(default=60, alias="RATE_LIMIT_WINDOW_SECONDS", description="Rate limit window")
    
    # CORS
    cors_origins: List[str] = Field(
        default=["http://localhost:3000", "http://localhost:8088"],
        description="Allowed CORS origins"
    )


class MonitoringSettings(BaseSettings):
    """Monitoring and Observability Configuration"""
    
    model_config = SettingsConfigDict(env_prefix="")
    
    prometheus_port: int = Field(default=9090, alias="PROMETHEUS_PORT", description="Prometheus port")
    grafana_port: int = Field(default=3000, alias="GRAFANA_PORT", description="Grafana port")
    
    # Logging
    log_level: str = Field(default="INFO", alias="LOG_LEVEL", description="Logging level")
    log_format: str = Field(default="json", description="Log format: json or text")
    log_file: Optional[str] = Field(default=None, description="Log file path")
    
    # Tracing
    enable_tracing: bool = Field(default=True, description="Enable OpenTelemetry tracing")
    trace_sample_rate: float = Field(default=0.1, description="Trace sample rate")


class DataQualitySettings(BaseSettings):
    """Data Quality Configuration"""
    
    model_config = SettingsConfigDict(env_prefix="")
    
    great_expectations_config_dir: str = Field(
        default="./great_expectations",
        alias="GREAT_EXPECTATIONS_CONFIG_DIR",
        description="Great Expectations config directory"
    )
    enable_data_quality_checks: bool = Field(
        default=True,
        alias="ENABLE_DATA_QUALITY_CHECKS",
        description="Enable data quality checks"
    )
    
    # Anomaly detection
    anomaly_detection_enabled: bool = Field(
        default=True,
        alias="ANOMALY_DETECTION_ENABLED",
        description="Enable anomaly detection"
    )
    anomaly_alert_threshold: float = Field(
        default=3.0,
        alias="ANOMALY_ALERT_THRESHOLD",
        description="Z-score threshold for anomaly detection"
    )
    anomaly_alert_email: Optional[str] = Field(
        default=None,
        alias="ANOMALY_ALERT_EMAIL",
        description="Email for anomaly alerts"
    )


class Settings(BaseSettings):
    """
    Main Application Settings
    
    Aggregates all configuration sections and provides a single entry point
    for accessing application configuration.
    """
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )
    
    # Application
    app_name: str = Field(default="ecommerce-analytics", alias="APP_NAME", description="Application name")
    app_env: str = Field(default="development", alias="APP_ENV", description="Environment")
    debug: bool = Field(default=False, alias="DEBUG", description="Debug mode")
    
    # API Server
    api_host: str = Field(default="0.0.0.0", alias="API_HOST", description="API host")
    api_port: int = Field(default=8000, alias="API_PORT", description="API port")
    api_workers: int = Field(default=4, alias="API_WORKERS", description="API workers")
    api_reload: bool = Field(default=False, alias="API_RELOAD", description="Enable reload")
    
    # Version
    version: str = Field(default="1.0.0", description="Application version")
    
    # Subsystem configurations
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    data_lake: DataLakeSettings = Field(default_factory=DataLakeSettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)
    monitoring: MonitoringSettings = Field(default_factory=MonitoringSettings)
    data_quality: DataQualitySettings = Field(default_factory=DataQualitySettings)
    
    @field_validator("app_env")
    @classmethod
    def validate_env(cls, v: str) -> str:
        """Validate environment value"""
        allowed = ["development", "staging", "production", "testing"]
        if v.lower() not in allowed:
            raise ValueError(f"Environment must be one of: {allowed}")
        return v.lower()
    
    @property
    def is_production(self) -> bool:
        """Check if running in production"""
        return self.app_env == "production"
    
    @property
    def is_development(self) -> bool:
        """Check if running in development"""
        return self.app_env == "development"


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached application settings.
    
    Uses LRU cache to ensure settings are only loaded once.
    
    Returns:
        Settings: Application settings instance
    """
    return Settings()


# Convenience function for accessing settings
settings = get_settings()
