from unittest.mock import AsyncMock

import pytest
from httpx import AsyncClient

from app.infrastructure.cache.redis import redis_manager
from app.infrastructure.database.session import database_manager


async def test_liveness(client: AsyncClient) -> None:
    response = await client.get("/api/v1/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_readiness_when_dependencies_are_available(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(database_manager, "ping", AsyncMock(return_value=None))
    monkeypatch.setattr(redis_manager, "ping", AsyncMock(return_value=None))

    response = await client.get("/api/v1/health/ready")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


async def test_readiness_returns_503_when_a_dependency_fails(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        database_manager, "ping", AsyncMock(side_effect=RuntimeError("unavailable"))
    )
    monkeypatch.setattr(redis_manager, "ping", AsyncMock(return_value=None))

    response = await client.get("/api/v1/health/ready")

    assert response.status_code == 503
    assert response.json() == {
        "status": "degraded",
        "database": {"status": "error"},
        "redis": {"status": "ok"},
    }
