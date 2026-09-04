from functools import lru_cache
from typing import Literal, Self

from pydantic import AliasChoices, Field, field_validator, model_validator
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
    connector_fast_operations_enabled: bool = False
    connector_fast_operation_queue_url: str | None = None
    connector_occurrence_queue_url: str | None = None
    connector_occurrence_dlq_arn: str | None = None
    connector_result_queue_url: str | None = None
    connector_scheduler_group: str | None = None
    connector_scheduler_role_arn: str | None = None
    connector_runtime_workspace_id: str | None = None
    connector_worker_poll_seconds: int = Field(default=1, ge=1, le=300)

    standardization_source_secret_id: str | None = None
    standardization_source_database: str = "elasticsearch"
    standardization_source_sslmode: Literal[
        "disable", "allow", "prefer", "require", "verify-ca", "verify-full"
    ] = Field(
        default="require",
        validation_alias=AliasChoices(
            "STANDARDIZATION_SOURCE_SSLMODE",
            "STANDARDIZATION_SOURCE_SSL",
        ),
    )
    standardization_dispatch_queue_url: str | None = None
    standardization_result_queue_url: str | None = None
    standardization_worker_poll_seconds: int = Field(default=10, ge=1, le=300)
    standardization_stale_partition_seconds: int = Field(
        default=900, ge=120, le=86400
    )

    @model_validator(mode="after")
    def validate_connector_fast_operation_queue(self) -> Self:
        if not self.connector_fast_operations_enabled:
            return self
        if not self.connector_fast_operation_queue_url:
            raise ValueError(
                "CONNECTOR_FAST_OPERATION_QUEUE_URL is required when "
                "CONNECTOR_FAST_OPERATIONS_ENABLED is enabled"
            )
        if not self.connector_fast_operation_queue_url.endswith(".fifo"):
            raise ValueError("CONNECTOR_FAST_OPERATION_QUEUE_URL must be an SQS FIFO URL")
        return self

    @field_validator("standardization_source_sslmode", mode="before")
    @classmethod
    def normalize_standardization_source_sslmode(cls, value: object) -> object:
        if isinstance(value, bool):
            return "require" if value else "disable"
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes", "on"}:
                return "require"
            if normalized in {"false", "0", "no", "off"}:
                return "disable"
            return normalized
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()
