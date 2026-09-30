"""Workspace app provenance can be assigned later without rewriting historical syncs."""

import uuid
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from httpx import ASGITransport, AsyncClient

from app.infrastructure.database.session import get_db_session
from app.main import create_app
from app.modules.connectors.model import Workspace


class Session:
    def __init__(self, workspace: Workspace | None, *, has_run: bool = False) -> None:
        self.workspace = workspace
        self.has_run = has_run
        self.commits = 0

    async def get(
        self, _model: type[Workspace], key: uuid.UUID, **_kwargs: object
    ) -> Workspace | None:
        return self.workspace if self.workspace and self.workspace.workspace_id == key else None

    async def execute(self, _statement: object) -> SimpleNamespace:
        return SimpleNamespace(scalar_one_or_none=lambda: uuid.uuid4() if self.has_run else None)

    async def commit(self) -> None:
        self.commits += 1

    async def refresh(self, row: Workspace) -> None:
        row.is_active = True
        row.created_at = row.updated_at = datetime.now(UTC)


@pytest.mark.asyncio
async def test_patch_workspace_sets_literal_app_id_and_get_reads_it_back() -> None:
    key = uuid.uuid4()
    session = Session(Workspace(workspace_id=key, slug="legacy", name="Legacy", app_id=None))
    app = create_app()
    app.dependency_overrides[get_db_session] = lambda: session
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        patched = await client.patch(f"/api/v1/workspaces/{key}", json={"app_id": "00042"})
        assert patched.status_code == 200, patched.text
        assert patched.json()["app_id"] == "00042"
        fetched = await client.get(f"/api/v1/workspaces/{key}")
        assert fetched.status_code == 200
        assert fetched.json()["app_id"] == "00042"
    assert session.commits == 1


@pytest.mark.asyncio
async def test_patch_workspace_is_idempotent_after_a_sync_run() -> None:
    key = uuid.uuid4()
    session = Session(
        Workspace(workspace_id=key, slug="used", name="Used", app_id="00042"), has_run=True
    )
    app = create_app()
    app.dependency_overrides[get_db_session] = lambda: session
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.patch(f"/api/v1/workspaces/{key}", json={"app_id": "00042"})
    assert response.status_code == 200
    assert response.json()["app_id"] == "00042"


@pytest.mark.asyncio
async def test_patch_workspace_rejects_rebinding_even_before_first_run() -> None:
    key = uuid.uuid4()
    session = Session(Workspace(workspace_id=key, slug="used", name="Used", app_id="42"))
    app = create_app()
    app.dependency_overrides[get_db_session] = lambda: session
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.patch(f"/api/v1/workspaces/{key}", json={"app_id": "00042"})
    assert response.status_code == 409
    assert session.workspace is not None and session.workspace.app_id == "42"
    assert session.commits == 0


@pytest.mark.asyncio
async def test_patch_workspace_rejects_app_change_after_a_sync_run() -> None:
    key = uuid.uuid4()
    session = Session(
        Workspace(workspace_id=key, slug="used", name="Used", app_id="123"), has_run=True
    )
    app = create_app()
    app.dependency_overrides[get_db_session] = lambda: session
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.patch(f"/api/v1/workspaces/{key}", json={"app_id": "456"})
    assert response.status_code == 409
    assert session.workspace is not None and session.workspace.app_id == "123"
    assert session.commits == 0


@pytest.mark.asyncio
async def test_patch_workspace_missing_returns_404() -> None:
    session = Session(None)
    app = create_app()
    app.dependency_overrides[get_db_session] = lambda: session
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.patch(f"/api/v1/workspaces/{uuid.uuid4()}", json={"app_id": "42"})
    assert response.status_code == 404
    assert session.commits == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [{}, {"app_id": None}, {"app_id": 42}, {"app_id": " 42"}, {"app_id": "42", "slug": "changed"}],
)
async def test_patch_workspace_rejects_invalid_payload(payload: dict[str, object]) -> None:
    key = uuid.uuid4()
    session = Session(Workspace(workspace_id=key, slug="untouched", name="Untouched", app_id="123"))
    app = create_app()
    app.dependency_overrides[get_db_session] = lambda: session
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.patch(f"/api/v1/workspaces/{key}", json=payload)
    assert response.status_code == 422
    assert session.commits == 0
