from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError
from sqlalchemy import or_, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.modules.connectors.model import (
    ConnectorConnection,
    ConnectorDataset,
    ConnectorOperation,
    ConnectorSchedule,
    MappingVersion,
    OutboxEvent,
    SchemaSnapshot,
    SyncRun,
    Workspace,
)
from app.modules.connectors.repository import ConnectorRepository
from app.modules.connectors.schemas import (
    ConnectionCreate,
    ConnectionUpdate,
    CredentialUpdate,
    DatasetUpdate,
    MappingCreate,
    ScheduleUpsert,
    WorkspaceCreate,
)

LOGGER = logging.getLogger(__name__)


class ConnectorNotFoundError(LookupError):
    pass


class ConnectorConflictError(RuntimeError):
    pass


class ActiveSyncRunError(ConnectorConflictError):
    pass


class ConnectorConfigurationError(ValueError):
    pass


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _physical_source_code(workspace_id: uuid.UUID, source_code: str) -> str:
    return f"w{workspace_id.hex[:8]}_{source_code}"[:63]


def _connection_constraint_conflict(exc: IntegrityError) -> str | None:
    diagnostic = getattr(exc.orig, "diag", None)
    constraint_name = getattr(diagnostic, "constraint_name", None)
    if constraint_name == "uq_connection_workspace_name":
        return "connector name already exists in this workspace"
    if constraint_name == "uq_connection_workspace_source":
        return "connector source_code already exists in this workspace"
    return None


def _connection_identity_conflict(
    existing: Sequence[ConnectorConnection], *, name: str, source_code: str
) -> str | None:
    if any(connection.name == name for connection in existing):
        return "connector name already exists in this workspace"
    if any(connection.source_code == source_code for connection in existing):
        return "connector source_code already exists in this workspace"
    return None


def _validate_mapping_source_paths(
    schema_json: dict[str, Any], payload: MappingCreate
) -> None:
    columns = schema_json.get("columns", [])
    available_paths = {
        str(column.get("name"))
        for column in columns
        if isinstance(column, dict) and column.get("name")
    }
    nested_roots = {
        str(column.get("name"))
        for column in columns
        if isinstance(column, dict)
        and column.get("name")
        and str(column.get("type", "")).lower()
        in {"json", "jsonb", "object", "document", "embedded document"}
    }
    invalid_paths = sorted(
        {
            field.source_path
            for field in payload.fields
            if field.source_path not in available_paths
            and not any(field.source_path.startswith(f"{root}.") for root in nested_roots)
        }
    )
    if invalid_paths:
        raise ConnectorConfigurationError(
            "mapping source_path values are not present in schema snapshot: "
            + ", ".join(invalid_paths)
        )


def _outbox(
    *, workspace_id: uuid.UUID, event_type: str, aggregate_id: uuid.UUID, payload: dict[str, Any]
) -> OutboxEvent:
    revision = payload.get("revision", 1)
    return OutboxEvent(
        workspace_id=workspace_id,
        event_type=event_type,
        aggregate_id=aggregate_id,
        deduplication_key=f"{event_type}:{aggregate_id}:{revision}",
        payload_json=payload,
    )


async def create_workspace(session: AsyncSession, payload: WorkspaceCreate) -> Workspace:
    workspace = Workspace(slug=payload.slug, name=payload.name)
    session.add(workspace)
    await session.commit()
    await session.refresh(workspace)
    return workspace


async def get_workspace(session: AsyncSession, workspace_id: uuid.UUID) -> Workspace:
    workspace = await ConnectorRepository(session).get_workspace(workspace_id)
    if workspace is None:
        raise ConnectorNotFoundError("workspace not found")
    return workspace


def _secret_name(settings: Settings, workspace_id: uuid.UUID, connection_id: uuid.UUID) -> str:
    prefix = settings.connector_secret_prefix.strip("/")
    return f"{prefix}/{settings.app_env}/{workspace_id}/{connection_id}"


async def _put_secret(
    settings: Settings,
    *,
    workspace_id: uuid.UUID,
    connection_id: uuid.UUID,
    engine: str,
    payload: dict[str, Any],
) -> tuple[str, bool]:
    client = boto3.client("secretsmanager", region_name=settings.aws_region)
    name = _secret_name(settings, workspace_id, connection_id)
    secret_value = json.dumps({"engine": engine, **payload}, separators=(",", ":"))

    def write() -> tuple[str, bool]:
        try:
            create_args: dict[str, Any] = {
                "Name": name,
                "SecretString": secret_value,
                "Description": "CDP database connector source credential",
                "Tags": [
                    {"Key": "application", "Value": "journey-builder-api"},
                    {"Key": "workspace-id", "Value": str(workspace_id)},
                    {"Key": "connection-id", "Value": str(connection_id)},
                ],
            }
            if settings.connector_secret_kms_key_id:
                create_args["KmsKeyId"] = settings.connector_secret_kms_key_id
            response = client.create_secret(**create_args)
            return str(response["ARN"]), True
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") != "ResourceExistsException":
                raise
            client.put_secret_value(SecretId=name, SecretString=secret_value)
            return str(client.describe_secret(SecretId=name)["ARN"]), False

    return await asyncio.to_thread(write)


async def _delete_secret(settings: Settings, secret_arn: str) -> None:
    client = boto3.client("secretsmanager", region_name=settings.aws_region)
    await asyncio.to_thread(
        client.delete_secret,
        SecretId=secret_arn,
        ForceDeleteWithoutRecovery=True,
    )


async def create_connection(
    session: AsyncSession,
    settings: Settings,
    workspace_id: uuid.UUID,
    payload: ConnectionCreate,
) -> ConnectorConnection:
    await get_workspace(session, workspace_id)
    existing = (
        await session.scalars(
            select(ConnectorConnection).where(
                ConnectorConnection.workspace_id == workspace_id,
                or_(
                    ConnectorConnection.name == payload.name,
                    ConnectorConnection.source_code == payload.source_code,
                ),
            )
        )
    ).all()
    identity_conflict = _connection_identity_conflict(
        existing, name=payload.name, source_code=payload.source_code
    )
    if identity_conflict:
        raise ConnectorConflictError(identity_conflict)

    connection = ConnectorConnection(
        workspace_id=workspace_id,
        definition_key=payload.engine,
        name=payload.name,
        source_code=payload.source_code,
        endpoint_label=payload.endpoint_label,
        safe_config=payload.safe_config,
        status="PROVISIONING",
    )
    session.add(connection)
    created_secret_arn: str | None = None
    try:
        await session.flush()
        secret_arn, created = await _put_secret(
            settings,
            workspace_id=workspace_id,
            connection_id=connection.connection_id,
            engine=payload.engine,
            payload=payload.credentials.secret_payload(),
        )
        connection.secret_arn = secret_arn
        if created:
            created_secret_arn = secret_arn
        connection.status = "DRAFT"
        await session.commit()
    except Exception as exc:
        await session.rollback()
        if created_secret_arn:
            try:
                await _delete_secret(settings, created_secret_arn)
            except ClientError:
                LOGGER.exception("failed to clean up connector secret after database rollback")
        if isinstance(exc, IntegrityError):
            conflict = _connection_constraint_conflict(exc)
            if conflict:
                raise ConnectorConflictError(conflict) from exc
        raise
    await session.refresh(connection)
    return connection


async def update_credentials(
    session: AsyncSession,
    settings: Settings,
    workspace_id: uuid.UUID,
    connection_id: uuid.UUID,
    payload: CredentialUpdate,
) -> ConnectorConnection:
    repository = ConnectorRepository(session)
    connection = await repository.get_connection(workspace_id, connection_id)
    if connection is None:
        raise ConnectorNotFoundError("connector not found")
    connection.secret_arn, _ = await _put_secret(
        settings,
        workspace_id=workspace_id,
        connection_id=connection_id,
        engine=connection.definition_key,
        payload=payload.credentials.secret_payload(),
    )
    connection.status = "DRAFT"
    connection.last_error_code = None
    await session.commit()
    await session.refresh(connection)
    return connection


async def update_connection(
    session: AsyncSession,
    workspace_id: uuid.UUID,
    connection_id: uuid.UUID,
    payload: ConnectionUpdate,
) -> ConnectorConnection:
    connection = await ConnectorRepository(session).get_connection(workspace_id, connection_id)
    if connection is None:
        raise ConnectorNotFoundError("connector not found")
    changes = payload.model_dump(exclude_unset=True)
    disabled = changes.pop("disabled", None)
    for key, value in changes.items():
        setattr(connection, key, value)
    if disabled is True:
        connection.status = "DISABLED"
        connection.disabled_at = _utcnow()
    elif disabled is False and connection.status == "DISABLED":
        connection.status = "DRAFT"
        connection.disabled_at = None
    await session.commit()
    await session.refresh(connection)
    return connection


async def queue_operation(
    session: AsyncSession,
    *,
    workspace_id: uuid.UUID,
    connection_id: uuid.UUID,
    operation_type: str,
    dataset_id: uuid.UUID | None = None,
    idempotency_key: str | None = None,
    trigger_type: str = "MANUAL",
    request_json: dict[str, Any] | None = None,
) -> ConnectorOperation:
    repository = ConnectorRepository(session)
    connection = await repository.get_connection(workspace_id, connection_id)
    if connection is None:
        raise ConnectorNotFoundError("connector not found")
    if connection.status == "DISABLED":
        raise ConnectorConflictError("connector is disabled")
    resolved_idempotency_key = idempotency_key or str(uuid.uuid4())
    existing = await session.scalar(
        select(ConnectorOperation).where(
            ConnectorOperation.workspace_id == workspace_id,
            ConnectorOperation.idempotency_key == resolved_idempotency_key,
        )
    )
    if existing is not None:
        resolved_request_json = request_json or {}
        if (
            existing.connection_id != connection_id
            or existing.dataset_id != dataset_id
            or existing.operation_type != operation_type
            or existing.request_json != resolved_request_json
        ):
            raise ConnectorConflictError("idempotency key was used for another operation")
        return existing
    operation = ConnectorOperation(
        workspace_id=workspace_id,
        connection_id=connection_id,
        dataset_id=dataset_id,
        operation_type=operation_type,
        trigger_type=trigger_type,
        idempotency_key=resolved_idempotency_key,
        request_json=request_json or {},
    )
    session.add(operation)
    await session.flush()
    session.add(
        _outbox(
            workspace_id=workspace_id,
            event_type="operation.dispatch",
            aggregate_id=operation.operation_id,
            payload={
                "message_version": 1,
                "operation_id": str(operation.operation_id),
                "operation_type": operation.operation_type,
                "workspace_id": str(workspace_id),
                "connection_id": str(connection_id),
                "dataset_id": str(dataset_id) if dataset_id else None,
                "revision": 1,
            },
        )
    )
    await session.commit()
    await session.refresh(operation)
    return operation


async def update_dataset(
    session: AsyncSession,
    workspace_id: uuid.UUID,
    dataset_id: uuid.UUID,
    payload: DatasetUpdate,
) -> ConnectorDataset:
    dataset = await ConnectorRepository(session).get_dataset(workspace_id, dataset_id)
    if dataset is None:
        raise ConnectorNotFoundError("dataset not found")
    changes = payload.model_dump(exclude_unset=True)
    for key, value in changes.items():
        setattr(dataset, key, value)
    if dataset.sync_mode == "INCREMENTAL" and not dataset.cursor_paths:
        raise ConnectorConfigurationError("incremental datasets require cursor_paths")
    if dataset.sync_mode == "INCREMENTAL" and not dataset.primary_key_paths:
        raise ConnectorConfigurationError("incremental datasets require primary_key_paths")
    await session.commit()
    await session.refresh(dataset)
    return dataset


async def create_mapping(
    session: AsyncSession,
    workspace_id: uuid.UUID,
    dataset_id: uuid.UUID,
    payload: MappingCreate,
) -> MappingVersion:
    repository = ConnectorRepository(session)
    dataset = await repository.get_dataset(workspace_id, dataset_id)
    if dataset is None:
        raise ConnectorNotFoundError("dataset not found")
    snapshot = await session.scalar(
        select(SchemaSnapshot).where(
            SchemaSnapshot.schema_snapshot_id == payload.schema_snapshot_id,
            SchemaSnapshot.dataset_id == dataset_id,
            SchemaSnapshot.workspace_id == workspace_id,
        )
    )
    if snapshot is None:
        raise ConnectorConfigurationError("schema snapshot does not belong to dataset")
    _validate_mapping_source_paths(snapshot.schema_json, payload)
    mapping_json = {"fields": [field.model_dump(mode="json") for field in payload.fields]}
    mapping = MappingVersion(
        workspace_id=workspace_id,
        dataset_id=dataset_id,
        version=await repository.next_mapping_version(dataset_id),
        schema_snapshot_id=payload.schema_snapshot_id,
        mapping_json=mapping_json,
        mapping_hash=_canonical_hash(mapping_json),
    )
    session.add(mapping)
    await session.commit()
    await session.refresh(mapping)
    return mapping


async def publish_mapping(
    session: AsyncSession, workspace_id: uuid.UUID, mapping_id: uuid.UUID
) -> MappingVersion:
    repository = ConnectorRepository(session)
    mapping = await repository.get_mapping(workspace_id, mapping_id)
    if mapping is None:
        raise ConnectorNotFoundError("mapping not found")
    if mapping.status == "PUBLISHED":
        return mapping
    dataset = await repository.get_dataset(workspace_id, mapping.dataset_id)
    if dataset is None:
        raise ConnectorNotFoundError("dataset not found")
    targets = {field["target_field"] for field in mapping.mapping_json.get("fields", [])}
    if "source_customer_id" not in targets:
        raise ConnectorConfigurationError("mapping requires source_customer_id")
    mapping.status = "PUBLISHED"
    mapping.published_at = _utcnow()
    dataset.active_mapping_version_id = mapping.mapping_version_id
    dataset.status = "MAPPED"
    await session.commit()
    await session.refresh(mapping)
    return mapping


async def upsert_schedule(
    session: AsyncSession,
    workspace_id: uuid.UUID,
    dataset_id: uuid.UUID,
    payload: ScheduleUpsert,
) -> ConnectorSchedule:
    repository = ConnectorRepository(session)
    dataset = await repository.get_dataset(workspace_id, dataset_id)
    if dataset is None:
        raise ConnectorNotFoundError("dataset not found")
    if dataset.active_mapping_version_id is None:
        raise ConnectorConfigurationError("publish a mapping before scheduling dataset")
    schedule = await repository.get_schedule(workspace_id, dataset_id)
    if schedule is None:
        schedule = ConnectorSchedule(
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            timing_json=payload.timing.model_dump(mode="json"),
            timezone=payload.timezone,
            start_date=payload.start_date,
            end_date=payload.end_date,
            status="PENDING_SYNC",
        )
        session.add(schedule)
        await session.flush()
    else:
        schedule.timing_json = payload.timing.model_dump(mode="json")
        schedule.timezone = payload.timezone
        schedule.start_date = payload.start_date
        schedule.end_date = payload.end_date
        schedule.status = "PENDING_SYNC"
        schedule.revision += 1
    session.add(
        _outbox(
            workspace_id=workspace_id,
            event_type="schedule.upsert",
            aggregate_id=schedule.schedule_id,
            payload={
                "message_version": 1,
                "schedule_id": str(schedule.schedule_id),
                "workspace_id": str(workspace_id),
                "dataset_id": str(dataset_id),
                "timing": schedule.timing_json,
                "timezone": schedule.timezone,
                "start_date": str(schedule.start_date) if schedule.start_date else None,
                "end_date": str(schedule.end_date) if schedule.end_date else None,
                "activate": payload.activate,
                "revision": schedule.revision,
            },
        )
    )
    await session.commit()
    await session.refresh(schedule)
    return schedule


async def control_schedule(
    session: AsyncSession, workspace_id: uuid.UUID, dataset_id: uuid.UUID, action: str
) -> ConnectorSchedule:
    schedule = await ConnectorRepository(session).get_schedule(workspace_id, dataset_id)
    if schedule is None:
        raise ConnectorNotFoundError("schedule not found")
    if action not in {"pause", "resume"}:
        raise ConnectorConfigurationError("unsupported schedule action")
    schedule.status = "PENDING_SYNC"
    schedule.revision += 1
    session.add(
        _outbox(
            workspace_id=workspace_id,
            event_type=f"schedule.{action}",
            aggregate_id=schedule.schedule_id,
            payload={
                "message_version": 1,
                "schedule_id": str(schedule.schedule_id),
                "action": action,
                "revision": schedule.revision,
            },
        )
    )
    await session.commit()
    await session.refresh(schedule)
    return schedule


async def materialize_sync_run(
    session: AsyncSession,
    settings: Settings,
    *,
    workspace_id: uuid.UUID,
    dataset_id: uuid.UUID,
    trigger_type: str,
    idempotency_key: str,
    commit: bool = True,
) -> tuple[ConnectorOperation, SyncRun]:
    if (
        settings.connector_runtime_workspace_id
        and settings.connector_runtime_workspace_id != str(workspace_id)
    ):
        raise ConnectorConflictError("workspace is not enabled for connector runtime")
    repository = ConnectorRepository(session)
    dataset = await repository.get_dataset(workspace_id, dataset_id)
    if dataset is None:
        raise ConnectorNotFoundError("dataset not found")
    if dataset.active_mapping_version_id is None:
        raise ConnectorConfigurationError("dataset has no published mapping")
    connection = await repository.get_connection(workspace_id, dataset.connection_id)
    if connection is None or connection.status != "READY":
        raise ConnectorConflictError("connector must be READY before sync")

    existing = await session.scalar(
        select(ConnectorOperation).where(
            ConnectorOperation.workspace_id == workspace_id,
            ConnectorOperation.idempotency_key == idempotency_key,
        )
    )
    if existing:
        run = await session.scalar(
            select(SyncRun).where(SyncRun.operation_id == existing.operation_id)
        )
        if run is None:
            raise ConnectorConflictError("idempotent operation exists without sync run")
        return existing, run

    active = await session.scalar(
        select(SyncRun).where(
            SyncRun.dataset_id == dataset_id,
            SyncRun.status.in_(["QUEUED", "EXTRACTING", "PUBLISHING", "INGESTING", "STITCHING"]),
        )
    )
    if active:
        raise ActiveSyncRunError("dataset already has an active sync run")

    physical_code = _physical_source_code(workspace_id, connection.source_code)
    if connection.source_system_id is None:
        result = await session.execute(
            text(
                """
                INSERT INTO control.source_system (source_code, source_name, source_type, is_active)
                VALUES (:source_code, :source_name, 'database_connector', true)
                ON CONFLICT (source_code) DO UPDATE SET is_active = true
                RETURNING source_system_id
                """
            ),
            {"source_code": physical_code, "source_name": connection.name},
        )
        connection.source_system_id = int(result.scalar_one())

    operation = ConnectorOperation(
        workspace_id=workspace_id,
        connection_id=connection.connection_id,
        dataset_id=dataset_id,
        operation_type="DATASET_SYNC",
        trigger_type=trigger_type,
        idempotency_key=idempotency_key,
    )
    session.add(operation)
    await session.flush()

    sync_run_id = uuid.uuid4()
    landing_batch_id = uuid.uuid4()
    ingestion_batch_id = uuid.uuid4()
    stitching_job_id = uuid.uuid4()
    batch_ref = f"db-{sync_run_id.hex}"
    output_key = (
        f"bronze/connectors/{workspace_id}/{connection.connection_id}/{dataset_id}/"
        f"{sync_run_id}/manifest.json"
    )
    lineage_params = {
        "landing_batch_id": landing_batch_id,
        "source_code": physical_code,
        "batch_ref": batch_ref,
        "output_key": output_key,
        "ingestion_batch_id": ingestion_batch_id,
        "source_system_id": connection.source_system_id,
        # asyncpg infers this bind as text from CAST(:sync_run_id AS text).
        # Pass a string rather than uuid.UUID to match the inferred parameter type.
        "sync_run_id": str(sync_run_id),
        "stitching_job_id": stitching_job_id,
    }
    await session.execute(
        text(
            """
            INSERT INTO control.source_landing_batch (
                batch_id, source_code, batch_ref, status, output_status,
                output_bucket, output_key, created_at, updated_at
            ) VALUES (
                :landing_batch_id, :source_code, :batch_ref, 'RECEIVING', 'PENDING',
                NULL, :output_key, now(), now()
            )
            """
        ),
        lineage_params,
    )
    await session.execute(
        text(
            """
            INSERT INTO control.ingestion_batch (
                batch_id, source_system_id, object_name, file_name, started_at,
                status, metadata, batch_ref, created_at, updated_at, landing_batch_id
            ) VALUES (
                :ingestion_batch_id, :source_system_id, :output_key, 'manifest.json', now(),
                'queued', jsonb_build_object(
                    'connector_sync_run_id', CAST(:sync_run_id AS text)
                ),
                :batch_ref, now(), now(), :landing_batch_id
            )
            """
        ),
        lineage_params,
    )
    await session.execute(
        text(
            """
            INSERT INTO control.stitching_job (
                job_id, ingestion_batch_id, attempt, status, created_at, updated_at
            ) VALUES (
                :stitching_job_id, :ingestion_batch_id, 1, 'QUEUED', now(), now()
            )
            """
        ),
        lineage_params,
    )
    run = SyncRun(
        sync_run_id=sync_run_id,
        operation_id=operation.operation_id,
        workspace_id=workspace_id,
        dataset_id=dataset_id,
        landing_batch_id=landing_batch_id,
        ingestion_batch_id=ingestion_batch_id,
        stitching_job_id=stitching_job_id,
        mapping_version_id=dataset.active_mapping_version_id,
    )
    session.add(run)
    await session.flush()
    session.add(
        _outbox(
            workspace_id=workspace_id,
            event_type="operation.dispatch",
            aggregate_id=operation.operation_id,
            payload={
                "message_version": 1,
                "operation_id": str(operation.operation_id),
                "operation_type": "DATASET_SYNC",
                "workspace_id": str(workspace_id),
                "connection_id": str(connection.connection_id),
                "dataset_id": str(dataset_id),
                "sync_run_id": str(sync_run_id),
                "landing_batch_id": str(landing_batch_id),
                "ingestion_batch_id": str(ingestion_batch_id),
                "stitching_job_id": str(stitching_job_id),
                "revision": 1,
            },
        )
    )
    if commit:
        await session.commit()
        await session.refresh(operation)
        await session.refresh(run)
    return operation, run
