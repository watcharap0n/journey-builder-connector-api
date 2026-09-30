"""Connector workspace ownership must preserve the literal upload app namespace."""

import uuid
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import ValidationError

from app.core.config import Settings
from app.infrastructure.database.session import get_db_session
from app.main import create_app
from app.modules.connectors.model import Workspace
from app.modules.connectors.schemas import WorkspaceCreate
from app.modules.connectors.service import ConnectorConfigurationError, materialize_sync_run


def test_workspace_creation_accepts_literal_numeric_app_id() -> None:
    payload = WorkspaceCreate.model_validate(
        {"slug": "tenant-a", "name": "Tenant A", "app_id": "00042"}
    )
    assert payload.app_id == "00042"


@pytest.mark.parametrize("app_id", [None, "", " 42", "42 ", "app-42", 42])
def test_workspace_creation_rejects_nonliteral_app_id(app_id: object) -> None:
    with pytest.raises(ValidationError):
        WorkspaceCreate.model_validate(
            {"slug": "tenant-a", "name": "Tenant A", "app_id": app_id}
        )


def test_workspace_creation_requires_app_id() -> None:
    with pytest.raises(ValidationError):
        WorkspaceCreate.model_validate({"slug": "tenant-a", "name": "Tenant A"})


def test_workspace_app_id_fits_literal_upload_batch_ref() -> None:
    assert WorkspaceCreate(slug="tenant-a", name="Tenant A", app_id="0" * 97).app_id == "0" * 97
    with pytest.raises(ValidationError):
        WorkspaceCreate(slug="tenant-a", name="Tenant A", app_id="0" * 98)


@pytest.mark.asyncio
async def test_post_workspace_without_auth_persists_and_get_returns_literal_app_id() -> None:
    class Session:
        def __init__(self) -> None:
            self.workspaces: dict[uuid.UUID, Workspace] = {}

        def add(self, row: Workspace) -> None:
            if row.workspace_id is None:
                row.workspace_id = uuid.uuid4()
            self.workspaces[row.workspace_id] = row

        async def commit(self) -> None:
            pass

        async def refresh(self, row: Workspace) -> None:
            row.created_at = row.updated_at = datetime.now(UTC)
            row.is_active = True

        async def get(self, _model: type[Workspace], key: uuid.UUID) -> Workspace | None:
            return self.workspaces.get(key)

    session = Session()
    app = create_app()
    app.dependency_overrides[get_db_session] = lambda: session
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        created = await client.post(
            "/api/v1/workspaces",
            json={"slug": "tenant-a", "name": "Tenant A", "app_id": "00042"},
        )
        assert created.status_code == 201
        workspace_id = created.json()["workspace_id"]
        assert created.json()["app_id"] == "00042"
        fetched = await client.get(f"/api/v1/workspaces/{workspace_id}")
        assert fetched.status_code == 200
        assert fetched.json()["app_id"] == "00042"
        assert session.workspaces[uuid.UUID(workspace_id)].app_id == "00042"


def test_workspace_column_retains_null_for_existing_rows_and_decimal_check() -> None:
    from sqlalchemy import CheckConstraint

    column = Workspace.__table__.c.app_id
    assert column.nullable
    assert str(column.type) == "TEXT"
    assert any(
        isinstance(constraint, CheckConstraint)
        and "app_id" in str(constraint.sqltext)
        and "^[0-9]+$" in str(constraint.sqltext)
        and constraint.name == "ck_workspace_workspace_app_id_decimal"
        for constraint in Workspace.__table__.constraints
    )

def test_workspace_migration_constraint_matches_model_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib.util
    from pathlib import Path

    from sqlalchemy import CheckConstraint

    from alembic import op

    path = Path(__file__).resolve().parents[1] / "alembic/versions/005_workspace_app_id.py"
    spec = importlib.util.spec_from_file_location("workspace_app_id_migration", path)
    assert spec and spec.loader
    revision = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(revision)
    actual: list[str] = []
    monkeypatch.setattr(op, "add_column", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(op, "f", lambda name: name)
    monkeypatch.setattr(
        op, "create_check_constraint",
        lambda name, *_args, **_kwargs: actual.append(name),
    )
    revision.upgrade()
    model = {
        constraint.name for constraint in Workspace.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert actual == ["ck_workspace_workspace_app_id_decimal"]
    assert actual[0] in model


@pytest.mark.asyncio
async def test_legacy_workspace_without_app_cannot_start_connector_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.modules.connectors import service

    workspace_id, dataset_id = uuid.uuid4(), uuid.uuid4()
    workspace = Workspace(workspace_id=workspace_id, slug="legacy", name="Legacy", app_id=None)

    class Repo:
        def __init__(self, session: object) -> None:
            pass

        async def get_dataset(self, _workspace_id: uuid.UUID, _dataset_id: uuid.UUID):
            return type("Dataset", (), {"active_mapping_version_id": uuid.uuid4()})()

        async def get_workspace(self, _workspace_id: uuid.UUID):
            return workspace

    class Session:
        async def get(self, _model: object, _key: uuid.UUID, **_kwargs: object):
            return workspace

        async def scalar(self, _sql: object):
            return None

        async def execute(self, _sql: object, _params: object = None):
            pytest.fail("no connector lineage should be created for unbound workspace")

    monkeypatch.setattr(service, "ConnectorRepository", Repo)
    with pytest.raises(ConnectorConfigurationError, match="app_id"):
        await materialize_sync_run(
            Session(), Settings(_env_file=None), workspace_id=workspace_id,
            dataset_id=dataset_id, trigger_type="MANUAL", idempotency_key="legacy-run",
        )
