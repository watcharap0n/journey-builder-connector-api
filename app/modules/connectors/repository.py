from __future__ import annotations

import uuid
from collections.abc import Sequence
from typing import cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.modules.connectors.model import (
    ConnectorConnection,
    ConnectorDataset,
    ConnectorOperation,
    ConnectorSchedule,
    MappingVersion,
    SyncRun,
    Workspace,
)


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
