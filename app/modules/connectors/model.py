from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.database.base import Base, TimestampMixin


class Workspace(Base, TimestampMixin):
    __tablename__ = "workspace"
    __table_args__ = (
        UniqueConstraint("slug", name="uq_workspace_slug"),
        {"schema": "platform"},
    )

    workspace_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    slug: Mapped[str] = mapped_column(String(63), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class ConnectorDefinition(Base):
    __tablename__ = "definition"
    __table_args__ = ({"schema": "connector"},)

    key: Mapped[str] = mapped_column(String(32), primary_key=True)
    family: Mapped[str] = mapped_column(String(32), nullable=False)
    adapter_key: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    capabilities: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class ConnectorConnection(Base, TimestampMixin):
    __tablename__ = "connection"
    __table_args__ = (
        UniqueConstraint("workspace_id", "name", name="uq_connection_workspace_name"),
        UniqueConstraint("workspace_id", "source_code", name="uq_connection_workspace_source"),
        CheckConstraint(
            "status IN ('PROVISIONING','DRAFT','TESTING','READY','ERROR','DISABLED')",
            name="connection_status",
        ),
        Index("ix_connection_workspace_status", "workspace_id", "status"),
        {"schema": "connector"},
    )

    connection_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("platform.workspace.workspace_id"), nullable=False
    )
    definition_key: Mapped[str] = mapped_column(
        ForeignKey("connector.definition.key"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_code: Mapped[str] = mapped_column(String(63), nullable=False)
    endpoint_label: Mapped[str | None] = mapped_column(String(255))
    secret_arn: Mapped[str | None] = mapped_column(Text)
    source_system_id: Mapped[int | None] = mapped_column(BigInteger)
    safe_config: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="PROVISIONING")
    last_tested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error_code: Mapped[str | None] = mapped_column(String(100))
    disabled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ConnectorDataset(Base, TimestampMixin):
    __tablename__ = "dataset"
    __table_args__ = (
        UniqueConstraint(
            "connection_id",
            "catalog_name",
            "namespace_name",
            "object_name",
            name="uq_dataset_source_object",
        ),
        CheckConstraint(
            "sync_mode IN ('FULL_SNAPSHOT','INCREMENTAL')", name="dataset_sync_mode"
        ),
        CheckConstraint(
            "status IN ('DISCOVERED','MAPPED','ACTIVE','PAUSED','DISABLED')",
            name="dataset_status",
        ),
        Index("ix_dataset_workspace_status", "workspace_id", "status"),
        {"schema": "connector"},
    )

    dataset_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("platform.workspace.workspace_id"), nullable=False
    )
    connection_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("connector.connection.connection_id"), nullable=False
    )
    catalog_name: Mapped[str | None] = mapped_column(String(255))
    namespace_name: Mapped[str | None] = mapped_column(String(255))
    object_name: Mapped[str] = mapped_column(String(255), nullable=False)
    target_entity: Mapped[str] = mapped_column(String(32), nullable=False, default="CUSTOMER")
    sync_mode: Mapped[str] = mapped_column(String(32), nullable=False, default="FULL_SNAPSHOT")
    primary_key_paths: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    cursor_paths: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    soft_delete_path: Mapped[str | None] = mapped_column(String(500))
    active_mapping_version_id: Mapped[uuid.UUID | None] = mapped_column()
    row_count_estimate: Mapped[int | None] = mapped_column(BigInteger)
    row_count_estimated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="DISCOVERED")


class SchemaSnapshot(Base):
    __tablename__ = "schema_snapshot"
    __table_args__ = (
        UniqueConstraint("dataset_id", "schema_hash", name="uq_schema_snapshot_hash"),
        Index("ix_schema_snapshot_dataset_discovered", "dataset_id", "discovered_at"),
        {"schema": "connector"},
    )

    schema_snapshot_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("platform.workspace.workspace_id"), nullable=False
    )
    dataset_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("connector.dataset.dataset_id"), nullable=False
    )
    schema_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    schema_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    compatibility_status: Mapped[str] = mapped_column(String(32), nullable=False, default="CURRENT")
    discovered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )


class MappingVersion(Base):
    __tablename__ = "mapping_version"
    __table_args__ = (
        UniqueConstraint("dataset_id", "version", name="uq_mapping_dataset_version"),
        CheckConstraint("status IN ('DRAFT','PUBLISHED','RETIRED')", name="mapping_status"),
        {"schema": "connector"},
    )

    mapping_version_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("platform.workspace.workspace_id"), nullable=False
    )
    dataset_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("connector.dataset.dataset_id"), nullable=False
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    schema_snapshot_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("connector.schema_snapshot.schema_snapshot_id"), nullable=False
    )
    mapping_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    mapping_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="DRAFT")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ConnectorSchedule(Base, TimestampMixin):
    __tablename__ = "schedule"
    __table_args__ = (
        UniqueConstraint("dataset_id", name="uq_schedule_dataset"),
        CheckConstraint(
            "status IN ('PENDING_SYNC','ACTIVE','PAUSED','ERROR','DISABLED')",
            name="schedule_status",
        ),
        Index("ix_schedule_workspace_status", "workspace_id", "status"),
        {"schema": "connector"},
    )

    schedule_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("platform.workspace.workspace_id"), nullable=False
    )
    dataset_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("connector.dataset.dataset_id"), nullable=False
    )
    timing_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    timezone: Mapped[str] = mapped_column(String(100), nullable=False)
    start_date: Mapped[date | None] = mapped_column(Date)
    end_date: Mapped[date | None] = mapped_column(Date)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="PENDING_SYNC")
    revision: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    aws_schedule_name: Mapped[str | None] = mapped_column(String(64), unique=True)
    aws_schedule_arn: Mapped[str | None] = mapped_column(Text)
    last_error_code: Mapped[str | None] = mapped_column(String(100))


class ConnectorOperation(Base, TimestampMixin):
    __tablename__ = "operation"
    __table_args__ = (
        UniqueConstraint("workspace_id", "idempotency_key", name="uq_operation_idempotency"),
        CheckConstraint(
            "operation_type IN "
            "('CONNECTION_TEST','SCHEMA_DISCOVERY','DATASET_PREVIEW','DATASET_SYNC')",
            name="operation_type",
        ),
        CheckConstraint(
            "status IN ('QUEUED','DISPATCHED','RUNNING','SUCCEEDED',"
            "'FAILED','SKIPPED','CANCELLED')",
            name="operation_status",
        ),
        Index("ix_operation_workspace_created", "workspace_id", "created_at"),
        {"schema": "connector"},
    )

    operation_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("platform.workspace.workspace_id"), nullable=False
    )
    connection_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("connector.connection.connection_id"), nullable=False
    )
    dataset_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("connector.dataset.dataset_id")
    )
    operation_type: Mapped[str] = mapped_column(String(32), nullable=False)
    trigger_type: Mapped[str] = mapped_column(String(20), nullable=False, default="MANUAL")
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="QUEUED")
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_code: Mapped[str | None] = mapped_column(String(100))
    error_message: Mapped[str | None] = mapped_column(Text)
    request_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    result_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    result_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ScheduleOccurrence(Base):
    __tablename__ = "schedule_occurrence"
    __table_args__ = (
        UniqueConstraint("schedule_id", "occurrence_key", name="uq_schedule_occurrence_key"),
        Index("ix_schedule_occurrence_time", "schedule_id", "scheduled_at"),
        {"schema": "connector"},
    )

    occurrence_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("platform.workspace.workspace_id"), nullable=False
    )
    schedule_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("connector.schedule.schedule_id"), nullable=False
    )
    occurrence_key: Mapped[str] = mapped_column(String(255), nullable=False)
    scheduled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    operation_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("connector.operation.operation_id")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )


class SyncRun(Base):
    __tablename__ = "sync_run"
    __table_args__ = (
        CheckConstraint(
            "status IN ('QUEUED','EXTRACTING','PUBLISHING','INGESTING','STITCHING',"
            "'SUCCEEDED','FAILED','SKIPPED','CANCELLED')",
            name="sync_run_status",
        ),
        Index(
            "uq_sync_run_active_dataset",
            "dataset_id",
            unique=True,
            postgresql_where=text(
                "status IN ('QUEUED','EXTRACTING','PUBLISHING','INGESTING','STITCHING')"
            ),
        ),
        Index("ix_sync_run_workspace_created", "workspace_id", "created_at"),
        {"schema": "connector"},
    )

    sync_run_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    operation_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("connector.operation.operation_id"), unique=True, nullable=False
    )
    workspace_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("platform.workspace.workspace_id"), nullable=False
    )
    dataset_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("connector.dataset.dataset_id"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="QUEUED")
    landing_batch_id: Mapped[uuid.UUID] = mapped_column(nullable=False, unique=True)
    ingestion_batch_id: Mapped[uuid.UUID] = mapped_column(nullable=False, unique=True)
    stitching_job_id: Mapped[uuid.UUID] = mapped_column(nullable=False, unique=True)
    mapping_version_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    checkpoint_from: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    checkpoint_to: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    manifest_bucket: Mapped[str | None] = mapped_column(Text)
    manifest_key: Mapped[str | None] = mapped_column(Text)
    records_read: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    records_loaded: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    records_rejected: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    bytes_written: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )

    @property
    def records_duplicate(self) -> int:
        return max(self.records_read - self.records_loaded - self.records_rejected, 0)


class SyncPartition(Base):
    __tablename__ = "sync_partition"
    __table_args__ = (
        UniqueConstraint("sync_run_id", "partition_no", name="uq_sync_partition_number"),
        Index("ix_sync_partition_status", "status", "updated_at"),
        {"schema": "connector"},
    )

    partition_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    sync_run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("connector.sync_run.sync_run_id"), nullable=False
    )
    partition_no: Mapped[int] = mapped_column(Integer, nullable=False)
    boundary_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="QUEUED")
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    s3_bucket: Mapped[str | None] = mapped_column(Text)
    s3_key: Mapped[str | None] = mapped_column(Text)
    checksum_sha256: Mapped[str | None] = mapped_column(String(64))
    row_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    byte_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    error_code: Mapped[str | None] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )


class ConnectorCheckpoint(Base):
    __tablename__ = "checkpoint"
    __table_args__ = ({"schema": "connector"},)

    dataset_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("connector.dataset.dataset_id"), primary_key=True
    )
    cursor_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    committed_sync_run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("connector.sync_run.sync_run_id"), nullable=False
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )


class RejectedConnectorRecord(Base):
    __tablename__ = "rejected_record"
    __table_args__ = (Index("ix_rejected_record_run", "sync_run_id"), {"schema": "connector"})

    rejected_record_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    sync_run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("connector.sync_run.sync_run_id"), nullable=False
    )
    partition_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("connector.sync_partition.partition_id")
    )
    source_record_key_hash: Mapped[str | None] = mapped_column(String(64))
    error_code: Mapped[str] = mapped_column(String(100), nullable=False)
    quarantine_s3_key: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )


class OutboxEvent(Base):
    __tablename__ = "outbox_event"
    __table_args__ = (
        UniqueConstraint("deduplication_key", name="uq_outbox_deduplication_key"),
        Index(
            "ix_outbox_pending",
            "status",
            "available_at",
            postgresql_where=text("status IN ('PENDING','FAILED','PROCESSING')"),
        ),
        {"schema": "connector"},
    )

    outbox_event_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("platform.workspace.workspace_id"), nullable=False
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    aggregate_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    deduplication_key: Mapped[str] = mapped_column(String(255), nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="PENDING")
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )
