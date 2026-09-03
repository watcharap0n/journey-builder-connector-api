from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator

ConnectorEngine = Literal["postgresql", "mysql", "mariadb", "mongodb"]
CUSTOMER_TARGET_FIELDS = {
    "source_customer_id",
    "external_id",
    "first_name",
    "middle_name",
    "last_name",
    "full_name",
    "nickname",
    "gender",
    "date_of_birth",
    "birthday",
    "age",
    "email",
    "phone",
    "line_user_id",
    "facebook_id",
    "google_id",
    "apple_id",
    "member_code",
    "customer_type",
    "status",
    "language",
    "address_line_1",
    "address_line_2",
    "district",
    "province",
    "postal_code",
    "country",
    "source",
    "source_system",
    "external_ids",
    "attributes",
    "source_created_at",
    "source_updated_at",
}


class ViewModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class WorkspaceCreate(BaseModel):
    slug: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    name: str = Field(min_length=1, max_length=255)


class WorkspaceView(ViewModel):
    workspace_id: uuid.UUID
    slug: str
    name: str
    is_active: bool
    created_at: datetime
    updated_at: datetime


class SshTunnelCredentials(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["ssh"] = "ssh"
    host: str = Field(min_length=1, max_length=255)
    port: int = Field(default=22, ge=1, le=65535)
    username: str = Field(min_length=1, max_length=255)
    private_key: SecretStr | None = None
    password: SecretStr | None = None
    private_key_passphrase: SecretStr | None = None
    host_key: str | None = Field(default=None, min_length=1, max_length=16_384)

    @model_validator(mode="after")
    def validate_authentication(self) -> SshTunnelCredentials:
        auth_methods = int(self.private_key is not None) + int(self.password is not None)
        if auth_methods != 1:
            raise ValueError("SSH tunnel requires exactly one of private_key or password")
        if self.private_key is not None and not self.private_key.get_secret_value().strip():
            raise ValueError("SSH private_key must not be empty")
        if self.password is not None and not self.password.get_secret_value():
            raise ValueError("SSH password must not be empty")
        if self.private_key_passphrase is not None and self.private_key is None:
            raise ValueError("private_key_passphrase requires private_key")
        return self

    def secret_payload(self) -> dict[str, Any]:
        payload = self.model_dump(
            exclude_none=True,
            exclude={"private_key", "password", "private_key_passphrase"},
        )
        if self.private_key is not None:
            payload["private_key"] = self.private_key.get_secret_value()
        if self.password is not None:
            payload["password"] = self.password.get_secret_value()
        if self.private_key_passphrase is not None:
            payload["private_key_passphrase"] = self.private_key_passphrase.get_secret_value()
        return payload


class SourceCredentials(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str | None = None
    hosts: list[str] = Field(default_factory=list)
    port: int | None = Field(default=None, ge=1, le=65535)
    username: str | None = None
    password: SecretStr | None = None
    database: str | None = None
    uri: SecretStr | None = None
    options: dict[str, Any] = Field(default_factory=dict)
    tunnel: SshTunnelCredentials | None = None

    @model_validator(mode="after")
    def validate_endpoint(self) -> SourceCredentials:
        if self.uri is None and self.host is None and not self.hosts:
            raise ValueError("credentials require uri, host, or hosts")
        if self.tunnel is not None and self.uri is not None:
            raise ValueError("SSH tunnel requires host or a single hosts entry, not uri")
        if self.tunnel is not None and len(self.hosts) > 1:
            raise ValueError("SSH tunnel does not support multiple database hosts")
        if self.tunnel is not None and self.host is None and len(self.hosts) != 1:
            raise ValueError("SSH tunnel requires exactly one database host")
        return self

    def secret_payload(self) -> dict[str, Any]:
        payload = self.model_dump(
            exclude_none=True,
            exclude={"password", "uri", "tunnel"},
        )
        if self.password is not None:
            payload["password"] = self.password.get_secret_value()
        if self.uri is not None:
            payload["uri"] = self.uri.get_secret_value()
        if self.tunnel is not None:
            payload["tunnel"] = self.tunnel.secret_payload()
        return payload


class ConnectionCreate(BaseModel):
    engine: ConnectorEngine
    name: str = Field(min_length=1, max_length=255)
    source_code: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{0,62}$")
    endpoint_label: str | None = Field(default=None, max_length=255)
    safe_config: dict[str, Any] = Field(default_factory=dict)
    credentials: SourceCredentials


class ConnectionUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=255)
    endpoint_label: str | None = Field(default=None, max_length=255)
    safe_config: dict[str, Any] | None = None
    disabled: bool | None = None


class CredentialUpdate(BaseModel):
    credentials: SourceCredentials


class ConnectionView(ViewModel):
    connection_id: uuid.UUID
    workspace_id: uuid.UUID
    definition_key: str
    name: str
    source_code: str
    endpoint_label: str | None
    source_system_id: int | None
    safe_config: dict[str, Any]
    status: str
    last_tested_at: datetime | None
    last_error_code: str | None
    created_at: datetime
    updated_at: datetime


class DatasetUpdate(BaseModel):
    sync_mode: Literal["FULL_SNAPSHOT", "INCREMENTAL"] | None = None
    primary_key_paths: list[str] | None = None
    cursor_paths: list[str] | None = None
    soft_delete_path: str | None = None
    status: Literal["DISCOVERED", "MAPPED", "ACTIVE", "PAUSED", "DISABLED"] | None = None

    @model_validator(mode="after")
    def validate_incremental(self) -> DatasetUpdate:
        if self.sync_mode == "INCREMENTAL" and not self.cursor_paths:
            raise ValueError("incremental datasets require cursor_paths")
        if self.sync_mode == "INCREMENTAL" and not self.primary_key_paths:
            raise ValueError("incremental datasets require primary_key_paths")
        return self


class DatasetView(ViewModel):
    dataset_id: uuid.UUID
    workspace_id: uuid.UUID
    connection_id: uuid.UUID
    catalog_name: str | None
    namespace_name: str | None
    object_name: str
    target_entity: str
    sync_mode: str
    primary_key_paths: list[str]
    cursor_paths: list[str]
    soft_delete_path: str | None
    active_mapping_version_id: uuid.UUID | None
    row_count_estimate: int | None
    row_count_estimated_at: datetime | None
    status: str
    created_at: datetime
    updated_at: datetime


class SchemaSnapshotView(ViewModel):
    schema_snapshot_id: uuid.UUID
    workspace_id: uuid.UUID
    dataset_id: uuid.UUID
    schema_hash: str
    schema_definition: dict[str, Any] = Field(
        validation_alias="schema_json", serialization_alias="schema_json"
    )
    compatibility_status: str
    discovered_at: datetime


class FieldMapping(BaseModel):
    source_path: str = Field(min_length=1, max_length=500)
    target_field: str = Field(min_length=1, max_length=100)
    transforms: list[
        Literal["trim", "lowercase", "uppercase", "parse_date", "parse_timestamp", "stringify"]
    ] = Field(default_factory=list)


class MappingCreate(BaseModel):
    schema_snapshot_id: uuid.UUID
    fields: list[FieldMapping] = Field(min_length=1)

    @model_validator(mode="after")
    def require_source_customer_id(self) -> MappingCreate:
        targets = {field.target_field for field in self.fields}
        if "source_customer_id" not in targets:
            raise ValueError("mapping requires target_field source_customer_id")
        if len(targets) != len(self.fields):
            raise ValueError("mapping target_field values must be unique")
        unsupported = sorted(targets - CUSTOMER_TARGET_FIELDS)
        if unsupported:
            raise ValueError("unsupported customer target_field: " + ", ".join(unsupported))
        return self


class MappingView(ViewModel):
    mapping_version_id: uuid.UUID
    workspace_id: uuid.UUID
    dataset_id: uuid.UUID
    version: int
    schema_snapshot_id: uuid.UUID
    mapping_json: dict[str, Any]
    mapping_hash: str
    status: str
    created_at: datetime
    published_at: datetime | None


class DatasetPreviewRequest(BaseModel):
    limit: int = Field(default=10, ge=1, le=50)


class RecurringTiming(BaseModel):
    frequency: Literal["daily", "weekly", "monthly"]
    time: str = Field(pattern=r"^(?:[01]\d|2[0-3]):[0-5]\d$")
    days_of_week: list[Literal["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]] = Field(
        default_factory=list
    )
    day_of_month: int | None = Field(default=None, ge=1, le=31)

    @model_validator(mode="after")
    def validate_frequency_fields(self) -> RecurringTiming:
        if self.frequency == "weekly" and not self.days_of_week:
            raise ValueError("weekly timing requires days_of_week")
        if len(set(self.days_of_week)) != len(self.days_of_week):
            raise ValueError("days_of_week values must be unique")
        if self.frequency == "monthly" and self.day_of_month is None:
            raise ValueError("monthly timing requires day_of_month")
        if self.frequency != "monthly" and self.day_of_month is not None:
            raise ValueError("day_of_month is only valid for monthly timing")
        return self


class ScheduleUpsert(BaseModel):
    timing: RecurringTiming
    timezone: str = "UTC"
    start_date: date | None = None
    end_date: date | None = None
    activate: bool = True

    @model_validator(mode="after")
    def validate_schedule(self) -> ScheduleUpsert:
        try:
            ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown IANA timezone '{self.timezone}'") from exc
        if self.start_date and self.end_date and self.end_date < self.start_date:
            raise ValueError("end_date must not be earlier than start_date")
        return self


class ScheduleView(ViewModel):
    schedule_id: uuid.UUID
    workspace_id: uuid.UUID
    dataset_id: uuid.UUID
    timing_json: dict[str, Any]
    timezone: str
    start_date: date | None
    end_date: date | None
    status: str
    revision: int
    aws_schedule_name: str | None
    aws_schedule_arn: str | None
    last_error_code: str | None
    created_at: datetime
    updated_at: datetime


class OperationAccepted(BaseModel):
    operation_id: uuid.UUID
    status: str


class OperationView(ViewModel):
    operation_id: uuid.UUID
    workspace_id: uuid.UUID
    connection_id: uuid.UUID
    dataset_id: uuid.UUID | None
    operation_type: str
    trigger_type: str
    status: str
    started_at: datetime | None
    completed_at: datetime | None
    error_code: str | None
    error_message: str | None
    result_json: dict[str, Any]
    result_expires_at: datetime | None
    created_at: datetime
    updated_at: datetime


class SyncRunView(ViewModel):
    sync_run_id: uuid.UUID
    operation_id: uuid.UUID
    workspace_id: uuid.UUID
    dataset_id: uuid.UUID
    status: str
    landing_batch_id: uuid.UUID
    ingestion_batch_id: uuid.UUID
    stitching_job_id: uuid.UUID
    mapping_version_id: uuid.UUID
    checkpoint_from: dict[str, Any] | None
    checkpoint_to: dict[str, Any] | None
    manifest_bucket: str | None
    manifest_key: str | None
    records_read: int
    records_loaded: int
    records_rejected: int
    bytes_written: int
    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime
