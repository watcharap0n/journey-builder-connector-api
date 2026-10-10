"""HTTP regression for a missing connector workspace."""

import uuid
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.core.config import Settings, get_settings
from app.infrastructure.database.session import get_db_session
from app.modules.connectors.controller import router


@pytest.mark.asyncio
async def test_missing_workspace_connector_list_returns_404() -> None:
    session = AsyncMock()
    session.get.return_value = None

    async def db_session():
        yield session

    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    app.dependency_overrides[get_db_session] = db_session
    app.dependency_overrides[get_settings] = lambda: Settings(_env_file=None)
    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        response = await client.get(f"/api/v1/workspaces/{uuid.uuid4()}/connectors")

    assert response.status_code == 404, response.text
    assert response.json() == {"detail": "workspace not found"}
