from functools import lru_cache

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        extra="ignore",
        case_sensitive=False,
        populate_by_name=True,
    )

    app_name: str = "Journey Builder API"
    app_version: str = "0.1.0"
    app_env: str = "local"
    debug: bool = False
    docs_enabled: bool = True
    log_level: str = "INFO"
    api_v1_prefix: str = "/api/v1"

    database_url: str | None = None
    database_echo: bool = False
    database_pool_size: int = Field(default=10, ge=1)
    database_max_overflow: int = Field(default=20, ge=0)

    redis_url: str = "redis://localhost:6379/0"
    cache_prefix: str = "journey-builder"
    cache_default_ttl_seconds: int = Field(default=300, ge=1)

    aws_region: str = Field(
        default="ap-southeast-1",
        validation_alias=AliasChoices("AWS_REGION", "AWS_DEFAULT_REGION"),
    )
    database_secret_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("DATABASE_SECRET_ID", "SECRET_ID"),
    )

    connector_secret_prefix: str = "connector"
    connector_secret_kms_key_id: str | None = None
    connector_dispatch_queue_url: str | None = None
    connector_occurrence_queue_url: str | None = None
    connector_occurrence_dlq_arn: str | None = None
    connector_result_queue_url: str | None = None
    connector_scheduler_group: str | None = None
    connector_scheduler_role_arn: str | None = None
    connector_runtime_workspace_id: str | None = None
    connector_worker_poll_seconds: int = Field(default=10, ge=1, le=300)


@lru_cache
def get_settings() -> Settings:
    return Settings()
