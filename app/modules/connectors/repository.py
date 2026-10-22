from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast

from sqlalchemy import (
    DateTime,
    Integer,
    SQLColumnExpression,
    String,
    Uuid,
    and_,
    case,
    column,
    func,
    literal,
    or_,
    select,
    table,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from app.modules.connectors.model import (
    ConnectorConnection,
    ConnectorDataset,
    ConnectorOperation,
    ConnectorSchedule,
    MappingVersion,
    SchemaSnapshot,
    SyncRun,
    Workspace,
)

# Read-only contract owned by aws-flex-deploy migration 058 (intake v2 upload outputs).
INTAKE_BATCH_OUTPUT = table(
    "intake_batch_output",
    column("dataset_id", Uuid),
    column("workspace_id", Uuid),
    column("batch_ref", String),
    column("object_key", String),
    column("target_entity", String),
    column("route", String),
    column("status", String),
    column("source_rows", Integer),
    column("output_rows", Integer),
    column("rejected_rows", Integer),
    column("skipped_rows", Integer),
    column("handoff_batch_ref", String),
    column("error", String),
    column("updated_at", DateTime(timezone=True)),
    schema="control",
)

# Read-only contract owned by aws-flex-deploy migration 025, deliberately not an ORM
# model (create_all must not synthesize a substitute). Missing schema fails closed.
PUBLICATION_STATUS = table(
    "customer_publication_status",
    column("ingestion_batch_id", Uuid),
    column("stitching_job_id", Uuid),
    column("overall_status", String),
    column("transfer_workspace_id", Uuid),
    column("transfer_updated_at", DateTime(timezone=True)),
    column("transfer_error_code", String),
    schema="control",
)


def _publication_status(execution_status: SQLColumnExpression[str]) -> ColumnElement[str]:
    # Post-ingestion states (or an explicit pipeline failure) supersede execution.
    # A pre-created QUEUED job is not proof: unfinished ingestion reports INGESTING.
    effective_stitching = and_(
        PUBLICATION_STATUS.c.stitching_job_id.is_not(None),
        PUBLICATION_STATUS.c.overall_status.in_(
            [
                "STITCHING_QUEUED", "WAITING_FOR_LOCK", "VALIDATING", "STITCHING",
                "STITCHING_UNKNOWN", "FAILED", "TRANSFER_PENDING", "TRANSFERRING",
                "TRANSFER_FAILED", "TRANSFER_BLOCKED", "COMPLETED", "COMPLETED_WITH_WARNINGS",
            ]
        ),
    )
    scoped_status = case(
        (
            or_(
                PUBLICATION_STATUS.c.transfer_workspace_id != SyncRun.workspace_id,
                and_(
                    PUBLICATION_STATUS.c.transfer_workspace_id.is_(None),
                    PUBLICATION_STATUS.c.overall_status.in_(
                        ["COMPLETED", "COMPLETED_WITH_WARNINGS"]
                    ),
                ),
            ),
            "TRANSFER_BLOCKED",
        ),
        else_=func.coalesce(PUBLICATION_STATUS.c.overall_status, "TRANSFER_PENDING"),
    )
    return case(
        (or_(execution_status == "SUCCEEDED", effective_stitching), scoped_status),
        else_=execution_status,
    )


@dataclass(frozen=True)
class PublicationProjection:
    status: str
    updated_at: datetime | None
    transfer_error_code: str | None


@dataclass(frozen=True)
class ConnectionSummary:
    connection: ConnectorConnection
    successful_records: int
    error_records: int
    duplicate_records: int
    frequency: str


class ConnectorRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_workspace(self, workspace_id: uuid.UUID) -> Workspace | None:
        return await self.session.get(Workspace, workspace_id)

    async def list_connections(self, workspace_id: uuid.UUID) -> Sequence[ConnectorConnection]:
        result = await self.session.scalars(
            select(ConnectorConnection)
            .where(ConnectorConnection.workspace_id == workspace_id)
            .order_by(ConnectorConnection.created_at.desc())
        )
        return result.all()

    async def list_connection_summaries(
        self, workspace_id: uuid.UUID, *, publication_enabled: bool = False
    ) -> Sequence[ConnectionSummary]:
        connections = await self.list_connections(workspace_id)
        # Preserve the existing execution-stage diagnostic counters even when
        # a later retry makes an originally failed run publicly successful.
        read_counter: SQLColumnExpression[int] = SyncRun.records_read
        loaded_counter: SQLColumnExpression[int] = SyncRun.records_loaded
        rejected_counter: SQLColumnExpression[int] = SyncRun.records_rejected
        successful = func.sum(SyncRun.records_loaded)
        if publication_enabled:
            read_counter = case((SyncRun.status == "SUCCEEDED", SyncRun.records_read), else_=0)
            loaded_counter = case((SyncRun.status == "SUCCEEDED", SyncRun.records_loaded), else_=0)
            rejected_counter = case(
                (SyncRun.status == "SUCCEEDED", SyncRun.records_rejected), else_=0
            )
            successful = func.sum(
                case(
                    (
                        _publication_status(SyncRun.status).in_(
                            ["COMPLETED", "COMPLETED_WITH_WARNINGS"]
                        ),
                        SyncRun.records_loaded,
                    ),
                    else_=0,
                )
            )
        run_query = (
            select(
                ConnectorDataset.connection_id,
                func.sum(read_counter).label("records_read"),
                func.sum(loaded_counter).label("records_loaded"),
                func.sum(rejected_counter).label("records_rejected"),
                successful.label("successful_records"),
            )
            .join(
                SyncRun,
                and_(
                    SyncRun.dataset_id == ConnectorDataset.dataset_id,
                    SyncRun.workspace_id == ConnectorDataset.workspace_id,
                ),
            )
            .where(ConnectorDataset.workspace_id == workspace_id)
            .group_by(ConnectorDataset.connection_id)
        )
        if publication_enabled:
            run_query = run_query.outerjoin(
                PUBLICATION_STATUS,
                PUBLICATION_STATUS.c.ingestion_batch_id == SyncRun.ingestion_batch_id,
            )
        else:
            run_query = run_query.where(SyncRun.status == "SUCCEEDED")
        run_rows = await self.session.execute(run_query)
        run_totals = {
            row.connection_id: (
                int(row.records_read or 0),
                int(row.records_loaded or 0),
                int(row.records_rejected or 0),
                int(row.successful_records or 0),
            )
            for row in run_rows
        }

        schedule_rows = await self.session.execute(
            select(ConnectorDataset.connection_id, ConnectorSchedule.timing_json).join(
                ConnectorSchedule,
                and_(
                    ConnectorSchedule.dataset_id == ConnectorDataset.dataset_id,
                    ConnectorSchedule.workspace_id == ConnectorDataset.workspace_id,
                ),
            )
            .where(
                ConnectorDataset.workspace_id == workspace_id,
                ConnectorSchedule.status != "DISABLED",
            )
        )
        frequencies: dict[uuid.UUID, set[str]] = {}
        for row in schedule_rows:
            frequency = str((row.timing_json or {}).get("frequency", ""))
            if frequency:
                frequencies.setdefault(row.connection_id, set()).add(frequency)

        summaries: list[ConnectionSummary] = []
        for connection in connections:
            records_read, records_loaded, records_rejected, successful_records = run_totals.get(
                connection.connection_id, (0, 0, 0, 0)
            )
            configured_frequencies = frequencies.get(connection.connection_id, set())
            frequency = (
                next(iter(configured_frequencies))
                if len(configured_frequencies) == 1
                else "mixed"
                if configured_frequencies
                else "manual"
            )
            summaries.append(
                ConnectionSummary(
                    connection=connection,
                    successful_records=successful_records,
                    error_records=records_rejected,
                    duplicate_records=max(
                        records_read - records_loaded - records_rejected, 0
                    ),
                    frequency=frequency,
                )
            )
        return summaries

    async def get_connection(
        self, workspace_id: uuid.UUID, connection_id: uuid.UUID
    ) -> ConnectorConnection | None:
        return cast(
            ConnectorConnection | None,
            await self.session.scalar(
                select(ConnectorConnection).where(
                    ConnectorConnection.workspace_id == workspace_id,
                    ConnectorConnection.connection_id == connection_id,
                )
            )
        )

    async def list_datasets(
        self, workspace_id: uuid.UUID, connection_id: uuid.UUID
    ) -> Sequence[ConnectorDataset]:
        result = await self.session.scalars(
            select(ConnectorDataset)
            .where(
                ConnectorDataset.workspace_id == workspace_id,
                ConnectorDataset.connection_id == connection_id,
            )
            .order_by(ConnectorDataset.object_name)
        )
        return result.all()

    async def get_dataset(
        self, workspace_id: uuid.UUID, dataset_id: uuid.UUID
    ) -> ConnectorDataset | None:
        return cast(
            ConnectorDataset | None,
            await self.session.scalar(
                select(ConnectorDataset).where(
                    ConnectorDataset.workspace_id == workspace_id,
                    ConnectorDataset.dataset_id == dataset_id,
                )
            )
        )

    async def list_schema_snapshots(
        self, workspace_id: uuid.UUID, dataset_id: uuid.UUID
    ) -> Sequence[SchemaSnapshot]:
        result = await self.session.scalars(
            select(SchemaSnapshot)
            .where(
                SchemaSnapshot.workspace_id == workspace_id,
                SchemaSnapshot.dataset_id == dataset_id,
            )
            .order_by(SchemaSnapshot.discovered_at.desc())
        )
        return result.all()

    async def get_mapping(
        self, workspace_id: uuid.UUID, mapping_id: uuid.UUID
    ) -> MappingVersion | None:
        return cast(
            MappingVersion | None,
            await self.session.scalar(
                select(MappingVersion).where(
                    MappingVersion.workspace_id == workspace_id,
                    MappingVersion.mapping_version_id == mapping_id,
                )
            )
        )

    async def next_mapping_version(self, dataset_id: uuid.UUID) -> int:
        latest = await self.session.scalar(
            select(MappingVersion.version)
            .where(MappingVersion.dataset_id == dataset_id)
            .order_by(MappingVersion.version.desc())
            .limit(1)
        )
        return int(latest or 0) + 1

    async def get_schedule(
        self, workspace_id: uuid.UUID, dataset_id: uuid.UUID
    ) -> ConnectorSchedule | None:
        return cast(
            ConnectorSchedule | None,
            await self.session.scalar(
                select(ConnectorSchedule).where(
                    ConnectorSchedule.workspace_id == workspace_id,
                    ConnectorSchedule.dataset_id == dataset_id,
                )
            )
        )

    async def get_operation(
        self, workspace_id: uuid.UUID, operation_id: uuid.UUID
    ) -> ConnectorOperation | None:
        return cast(
            ConnectorOperation | None,
            await self.session.scalar(
                select(ConnectorOperation).where(
                    ConnectorOperation.workspace_id == workspace_id,
                    ConnectorOperation.operation_id == operation_id,
                )
            )
        )

    async def get_operation_publication_projection(
        self, workspace_id: uuid.UUID, operation_id: uuid.UUID, *, execution_status: str
    ) -> PublicationProjection:
        result = await self.session.execute(
            select(
                _publication_status(literal(execution_status)).label("status"),
                func.greatest(
                    ConnectorOperation.updated_at, PUBLICATION_STATUS.c.transfer_updated_at
                ).label("updated_at"),
                PUBLICATION_STATUS.c.transfer_error_code,
            )
            .select_from(SyncRun)
            .join(
                ConnectorOperation,
                and_(
                    ConnectorOperation.operation_id == SyncRun.operation_id,
                    ConnectorOperation.workspace_id == SyncRun.workspace_id,
                ),
            )
            .outerjoin(
                PUBLICATION_STATUS,
                PUBLICATION_STATUS.c.ingestion_batch_id == SyncRun.ingestion_batch_id,
            )
            .where(SyncRun.workspace_id == workspace_id, SyncRun.operation_id == operation_id)
        )
        projected = result.one_or_none()
        if projected is not None:
            return PublicationProjection(
                status=projected.status,
                updated_at=projected.updated_at,
                transfer_error_code=projected.transfer_error_code,
            )
        return PublicationProjection(
            status="TRANSFER_PENDING" if execution_status == "SUCCEEDED" else execution_status,
            updated_at=None,
            transfer_error_code=None,
        )

    async def get_sync_run(
        self, workspace_id: uuid.UUID, sync_run_id: uuid.UUID
    ) -> SyncRun | None:
        return cast(
            SyncRun | None,
            await self.session.scalar(
                select(SyncRun).where(
                    SyncRun.workspace_id == workspace_id,
                    SyncRun.sync_run_id == sync_run_id,
                )
            )
        )


async def list_intake_outputs(
    session: AsyncSession, workspace_id: uuid.UUID, dataset_id: uuid.UUID, batch_ref: str | None
) -> list[dict[str, Any]]:
    query = select(INTAKE_BATCH_OUTPUT).where(
        INTAKE_BATCH_OUTPUT.c.workspace_id == workspace_id,
        INTAKE_BATCH_OUTPUT.c.dataset_id == dataset_id,
    )
    if batch_ref:
        query = query.where(INTAKE_BATCH_OUTPUT.c.batch_ref == batch_ref)
    query = query.order_by(INTAKE_BATCH_OUTPUT.c.updated_at.desc()).limit(200)
    return [dict(row) for row in (await session.execute(query)).mappings().all()]
