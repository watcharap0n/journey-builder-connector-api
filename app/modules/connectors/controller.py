from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings, get_settings
from app.infrastructure.database.session import get_db_session
from app.modules.connectors.repository import ConnectorRepository
from app.modules.connectors.schemas import (
    ConnectionCreate,
    ConnectionUpdate,
    ConnectionView,
    CredentialUpdate,
    DatasetPreviewRequest,
    DatasetUpdate,
    DatasetView,
    MappingCreate,
    MappingView,
    OperationAccepted,
    OperationView,
    ScheduleUpsert,
    ScheduleView,
    SchemaSnapshotView,
    SyncRunView,
    WorkspaceCreate,
    WorkspaceView,
)
from app.modules.connectors.service import (
    ConnectorConfigurationError,
    ConnectorConflictError,
    ConnectorNotFoundError,
    control_schedule,
    create_connection,
    create_mapping,
    create_workspace,
    get_workspace,
    materialize_sync_run,
    publish_mapping,
    queue_operation,
    update_connection,
    update_credentials,
    update_dataset,
    upsert_schedule,
)

router = APIRouter(tags=["database-connectors"])
SessionDep = Annotated[AsyncSession, Depends(get_db_session)]
SettingsDep = Annotated[Settings, Depends(get_settings)]
IdempotencyKey = Annotated[str | None, Header(alias="Idempotency-Key")]


def _hide_expired_preview_result(view: OperationView, *, now: datetime) -> OperationView:
    if (
        view.operation_type == "DATASET_PREVIEW"
        and view.result_expires_at is not None
        and view.result_expires_at <= now
    ):
        return view.model_copy(update={"result_json": {"expired": True}})
    return view


def _translate_error(exc: Exception) -> HTTPException:
    if isinstance(exc, ConnectorNotFoundError):
        return HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, ConnectorConflictError):
        return HTTPException(status_code=409, detail=str(exc))
    if isinstance(exc, ConnectorConfigurationError):
        return HTTPException(status_code=422, detail=str(exc))
    return HTTPException(status_code=500, detail="connector operation failed")


@router.post("/workspaces", response_model=WorkspaceView, status_code=status.HTTP_201_CREATED)
async def post_workspace(
    payload: WorkspaceCreate, session: SessionDep
) -> WorkspaceView:
    return WorkspaceView.model_validate(await create_workspace(session, payload))


@router.get("/workspaces/{workspace_id}", response_model=WorkspaceView)
async def get_workspace_by_id(
    workspace_id: uuid.UUID, session: SessionDep
) -> WorkspaceView:
    try:
        return WorkspaceView.model_validate(await get_workspace(session, workspace_id))
    except Exception as exc:
        raise _translate_error(exc) from exc


@router.post(
    "/workspaces/{workspace_id}/connectors",
    response_model=ConnectionView,
    status_code=status.HTTP_201_CREATED,
)
async def post_connection(
    workspace_id: uuid.UUID,
    payload: ConnectionCreate,
    session: SessionDep,
    settings: SettingsDep,
) -> ConnectionView:
    try:
        connection = await create_connection(session, settings, workspace_id, payload)
        return ConnectionView.model_validate(connection)
    except Exception as exc:
        raise _translate_error(exc) from exc


@router.get(
    "/workspaces/{workspace_id}/connectors", response_model=list[ConnectionView]
)
async def get_connections(
    workspace_id: uuid.UUID, session: SessionDep
) -> Sequence[ConnectionView]:
    await get_workspace(session, workspace_id)
    rows = await ConnectorRepository(session).list_connections(workspace_id)
    return [ConnectionView.model_validate(row) for row in rows]


@router.patch(
    "/workspaces/{workspace_id}/connectors/{connection_id}",
    response_model=ConnectionView,
)
async def patch_connection(
    workspace_id: uuid.UUID,
    connection_id: uuid.UUID,
    payload: ConnectionUpdate,
    session: SessionDep,
) -> ConnectionView:
    try:
        row = await update_connection(session, workspace_id, connection_id, payload)
        return ConnectionView.model_validate(row)
    except Exception as exc:
        raise _translate_error(exc) from exc


@router.put(
    "/workspaces/{workspace_id}/connectors/{connection_id}/credentials",
    response_model=ConnectionView,
)
async def put_credentials(
    workspace_id: uuid.UUID,
    connection_id: uuid.UUID,
    payload: CredentialUpdate,
    session: SessionDep,
    settings: SettingsDep,
) -> ConnectionView:
    try:
        row = await update_credentials(
            session, settings, workspace_id, connection_id, payload
        )
        return ConnectionView.model_validate(row)
    except Exception as exc:
        raise _translate_error(exc) from exc


async def _queue_connection_operation(
    operation_type: str,
    workspace_id: uuid.UUID,
    connection_id: uuid.UUID,
    session: AsyncSession,
    idempotency_key: str | None,
) -> OperationAccepted:
    try:
        operation = await queue_operation(
            session,
            workspace_id=workspace_id,
            connection_id=connection_id,
            operation_type=operation_type,
            idempotency_key=idempotency_key,
        )
        return OperationAccepted(operation_id=operation.operation_id, status=operation.status)
    except Exception as exc:
        raise _translate_error(exc) from exc


@router.get(
    "/workspaces/{workspace_id}/datasets/{dataset_id}/schema-snapshots",
    response_model=list[SchemaSnapshotView],
)
async def get_dataset_schema_snapshots(
    workspace_id: uuid.UUID,
    dataset_id: uuid.UUID,
    session: SessionDep,
) -> Sequence[SchemaSnapshotView]:
    repository = ConnectorRepository(session)
    if await repository.get_dataset(workspace_id, dataset_id) is None:
        raise HTTPException(status_code=404, detail="dataset not found")
    rows = await repository.list_schema_snapshots(workspace_id, dataset_id)
    return [SchemaSnapshotView.model_validate(row) for row in rows]


@router.post(
    "/workspaces/{workspace_id}/connectors/{connection_id}/test",
    response_model=OperationAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def post_connection_test(
    workspace_id: uuid.UUID,
    connection_id: uuid.UUID,
    session: SessionDep,
    idempotency_key: IdempotencyKey = None,
) -> OperationAccepted:
    return await _queue_connection_operation(
        "CONNECTION_TEST", workspace_id, connection_id, session, idempotency_key
    )


@router.post(
    "/workspaces/{workspace_id}/connectors/{connection_id}/discover",
    response_model=OperationAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def post_discovery(
    workspace_id: uuid.UUID,
    connection_id: uuid.UUID,
    session: SessionDep,
    idempotency_key: IdempotencyKey = None,
) -> OperationAccepted:
    return await _queue_connection_operation(
        "SCHEMA_DISCOVERY", workspace_id, connection_id, session, idempotency_key
    )


@router.get(
    "/workspaces/{workspace_id}/connectors/{connection_id}/datasets",
    response_model=list[DatasetView],
)
async def get_datasets(
    workspace_id: uuid.UUID,
    connection_id: uuid.UUID,
    session: SessionDep,
) -> Sequence[DatasetView]:
    repository = ConnectorRepository(session)
    if await repository.get_connection(workspace_id, connection_id) is None:
        raise HTTPException(status_code=404, detail="connector not found")
    rows = await repository.list_datasets(workspace_id, connection_id)
    return [DatasetView.model_validate(row) for row in rows]


@router.patch(
    "/workspaces/{workspace_id}/datasets/{dataset_id}", response_model=DatasetView
)
async def patch_dataset(
    workspace_id: uuid.UUID,
    dataset_id: uuid.UUID,
    payload: DatasetUpdate,
    session: SessionDep,
) -> DatasetView:
    try:
        row = await update_dataset(session, workspace_id, dataset_id, payload)
        return DatasetView.model_validate(row)
    except Exception as exc:
        raise _translate_error(exc) from exc


@router.post(
    "/workspaces/{workspace_id}/datasets/{dataset_id}/preview",
    response_model=OperationAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def post_dataset_preview(
    workspace_id: uuid.UUID,
    dataset_id: uuid.UUID,
    payload: DatasetPreviewRequest,
    session: SessionDep,
    idempotency_key: IdempotencyKey = None,
) -> OperationAccepted:
    try:
        dataset = await ConnectorRepository(session).get_dataset(workspace_id, dataset_id)
        if dataset is None:
            raise ConnectorNotFoundError("dataset not found")
        operation = await queue_operation(
            session,
            workspace_id=workspace_id,
            connection_id=dataset.connection_id,
            dataset_id=dataset_id,
            operation_type="DATASET_PREVIEW",
            idempotency_key=idempotency_key,
            request_json={"limit": payload.limit},
        )
        return OperationAccepted(operation_id=operation.operation_id, status=operation.status)
    except Exception as exc:
        raise _translate_error(exc) from exc


@router.post(
    "/workspaces/{workspace_id}/datasets/{dataset_id}/mappings",
    response_model=MappingView,
    status_code=status.HTTP_201_CREATED,
)
async def post_mapping(
    workspace_id: uuid.UUID,
    dataset_id: uuid.UUID,
    payload: MappingCreate,
    session: SessionDep,
) -> MappingView:
    try:
        row = await create_mapping(session, workspace_id, dataset_id, payload)
        return MappingView.model_validate(row)
    except Exception as exc:
        raise _translate_error(exc) from exc


@router.post(
    "/workspaces/{workspace_id}/mappings/{mapping_id}/publish",
    response_model=MappingView,
)
async def post_mapping_publish(
    workspace_id: uuid.UUID,
    mapping_id: uuid.UUID,
    session: SessionDep,
) -> MappingView:
    try:
        row = await publish_mapping(session, workspace_id, mapping_id)
        return MappingView.model_validate(row)
    except Exception as exc:
        raise _translate_error(exc) from exc


@router.put(
    "/workspaces/{workspace_id}/datasets/{dataset_id}/schedule",
    response_model=ScheduleView,
)
async def put_schedule(
    workspace_id: uuid.UUID,
    dataset_id: uuid.UUID,
    payload: ScheduleUpsert,
    session: SessionDep,
) -> ScheduleView:
    try:
        row = await upsert_schedule(session, workspace_id, dataset_id, payload)
        return ScheduleView.model_validate(row)
    except Exception as exc:
        raise _translate_error(exc) from exc


@router.get(
    "/workspaces/{workspace_id}/datasets/{dataset_id}/schedule",
    response_model=ScheduleView,
)
async def get_dataset_schedule(
    workspace_id: uuid.UUID,
    dataset_id: uuid.UUID,
    session: SessionDep,
) -> ScheduleView:
    row = await ConnectorRepository(session).get_schedule(workspace_id, dataset_id)
    if row is None:
        raise HTTPException(status_code=404, detail="schedule not found")
    return ScheduleView.model_validate(row)


@router.post(
    "/workspaces/{workspace_id}/datasets/{dataset_id}/schedule/{action}",
    response_model=ScheduleView,
)
async def post_schedule_action(
    workspace_id: uuid.UUID,
    dataset_id: uuid.UUID,
    action: str,
    session: SessionDep,
) -> ScheduleView:
    try:
        row = await control_schedule(session, workspace_id, dataset_id, action)
        return ScheduleView.model_validate(row)
    except Exception as exc:
        raise _translate_error(exc) from exc


@router.post(
    "/workspaces/{workspace_id}/datasets/{dataset_id}/runs",
    response_model=SyncRunView,
    status_code=status.HTTP_202_ACCEPTED,
)
async def post_run_now(
    workspace_id: uuid.UUID,
    dataset_id: uuid.UUID,
    session: SessionDep,
    settings: SettingsDep,
    idempotency_key: IdempotencyKey = None,
) -> SyncRunView:
    try:
        _, run = await materialize_sync_run(
            session,
            settings,
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            trigger_type="MANUAL",
            idempotency_key=idempotency_key or str(uuid.uuid4()),
        )
        return SyncRunView.model_validate(run)
    except Exception as exc:
        raise _translate_error(exc) from exc


@router.get(
    "/workspaces/{workspace_id}/operations/{operation_id}", response_model=OperationView
)
async def get_operation(
    workspace_id: uuid.UUID,
    operation_id: uuid.UUID,
    session: SessionDep,
) -> OperationView:
    row = await ConnectorRepository(session).get_operation(workspace_id, operation_id)
    if row is None:
        raise HTTPException(status_code=404, detail="operation not found")
    view = OperationView.model_validate(row)
    return _hide_expired_preview_result(view, now=datetime.now(UTC))


@router.get(
    "/workspaces/{workspace_id}/runs/{sync_run_id}", response_model=SyncRunView
)
async def get_run(
    workspace_id: uuid.UUID,
    sync_run_id: uuid.UUID,
    session: SessionDep,
) -> SyncRunView:
    row = await ConnectorRepository(session).get_sync_run(workspace_id, sync_run_id)
    if row is None:
        raise HTTPException(status_code=404, detail="sync run not found")
    return SyncRunView.model_validate(row)


@router.delete(
    "/workspaces/{workspace_id}/connectors/{connection_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def disable_connection(
    workspace_id: uuid.UUID,
    connection_id: uuid.UUID,
    session: SessionDep,
) -> Response:
    try:
        await update_connection(
            session, workspace_id, connection_id, ConnectionUpdate(disabled=True)
        )
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as exc:
        raise _translate_error(exc) from exc
