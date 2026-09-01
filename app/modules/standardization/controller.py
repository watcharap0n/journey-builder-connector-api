from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings, get_settings
from app.infrastructure.database.session import get_db_session
from app.modules.standardization.schemas import StandardizationRunView
from app.modules.standardization.service import (
    StandardizationConfigurationError,
    StandardizationNotFoundError,
    create_manual_run,
    discover_source_inventory,
    get_run,
)

router = APIRouter(tags=["standardization"])
SessionDep = Annotated[AsyncSession, Depends(get_db_session)]
SettingsDep = Annotated[Settings, Depends(get_settings)]
IdempotencyKey = Annotated[str | None, Header(alias="Idempotency-Key")]


def _translate_error(exc: Exception) -> HTTPException:
    if isinstance(exc, StandardizationNotFoundError):
        return HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, StandardizationConfigurationError):
        return HTTPException(status_code=503, detail=str(exc))
    return HTTPException(status_code=500, detail="standardization operation failed")


@router.post(
    "/workspaces/{workspace_id}/standardization-datasets/{dataset_id}/runs",
    response_model=StandardizationRunView,
    status_code=status.HTTP_202_ACCEPTED,
)
async def post_standardization_run(
    workspace_id: uuid.UUID,
    dataset_id: int,
    session: SessionDep,
    settings: SettingsDep,
    idempotency_key: IdempotencyKey = None,
) -> StandardizationRunView:
    try:
        inventory = await discover_source_inventory(settings)
        return await create_manual_run(
            session,
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            inventory=inventory,
            idempotency_key=idempotency_key,
        )
    except Exception as exc:
        raise _translate_error(exc) from exc


@router.get(
    "/workspaces/{workspace_id}/standardization-runs/{run_id}",
    response_model=StandardizationRunView,
)
async def get_standardization_run(
    workspace_id: uuid.UUID,
    run_id: uuid.UUID,
    session: SessionDep,
) -> StandardizationRunView:
    try:
        return await get_run(session, workspace_id, run_id)
    except Exception as exc:
        raise _translate_error(exc) from exc

