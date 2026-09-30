"""Connector sync binds the workspace app to durable batch lineage before dispatch."""

import uuid
from types import SimpleNamespace
from typing import Any

import pytest

from app.core.config import Settings
from app.modules.connectors import service
from app.modules.connectors.model import ConnectorConnection, ConnectorDataset, Workspace


@pytest.mark.asyncio
async def test_connector_sync_assigns_app_after_registering_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace_id = uuid.uuid4()
    dataset_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    workspace = Workspace(workspace_id=workspace_id, slug="tenant", name="Tenant", app_id="00042")
    dataset = ConnectorDataset(
        dataset_id=dataset_id, workspace_id=workspace_id, connection_id=connection_id,
        object_name="customers", active_mapping_version_id=uuid.uuid4(),
    )
    connection = ConnectorConnection(
        connection_id=connection_id, workspace_id=workspace_id, definition_key="postgresql",
        name="Customers", source_code="customers", source_system_id=None, status="READY",
    )

    class Repo:
        def __init__(self, session: object) -> None:
            pass

        async def get_workspace(self, _workspace: uuid.UUID):
            return workspace

        async def get_dataset(self, _workspace: uuid.UUID, _dataset: uuid.UUID):
            return dataset

        async def get_connection(self, _workspace: uuid.UUID, _connection: uuid.UUID):
            return connection

    class Session:
        def __init__(self) -> None:
            self.statements: list[tuple[str, Any]] = []
            self.added: list[Any] = []

        async def get(self, _model: object, _key: uuid.UUID, **_kwargs: object):
            return workspace

        async def scalar(self, _statement: object):
            return None

        async def execute(self, statement: object, params: Any):
            self.statements.append((str(statement), params))
            value = 11 if "INSERT INTO control.source_system" in str(statement) else "00042"
            return SimpleNamespace(scalar_one=lambda: value, scalar_one_or_none=lambda: value)

        def add(self, value: Any) -> None:
            self.added.append(value)

        async def flush(self) -> None:
            for value in self.added:
                for attribute in ("operation_id", "event_id"):
                    if hasattr(value, attribute) and getattr(value, attribute) is None:
                        setattr(value, attribute, uuid.uuid4())

    monkeypatch.setattr(service, "ConnectorRepository", Repo)
    session = Session()
    _, run = await service.materialize_sync_run(
        session, Settings(_env_file=None), workspace_id=workspace_id, dataset_id=dataset_id,
        trigger_type="MANUAL", idempotency_key="app-42", commit=False,
    )
    ingestion = next(
        (sql, params) for sql, params in session.statements
        if "INSERT INTO control.ingestion_batch" in sql
    )
    binding = next(
        (sql, params) for sql, params in session.statements
        if "bind_connector_batch_app_id" in sql
    )
    assert ingestion[1]["batch_ref"] == f"db-{run.sync_run_id.hex}"
    assert session.statements.index(binding) > session.statements.index(ingestion)
    assert binding[1]["app_id"] == "00042"
    assert binding[1]["ingestion_batch_id"] == run.ingestion_batch_id
    assert binding[1]["workspace_id"] == workspace_id
    source = next(
        (sql, params) for sql, params in session.statements
        if "INSERT INTO control.source_system" in sql
    )
    assert source[1]["source_code"] == f"dc1-{connection_id.hex}"
    assert "DO NOTHING" in source[0]
    assert any("INSERT INTO control.stitching_job" in sql for sql, _ in session.statements)


@pytest.mark.asyncio
async def test_existing_source_registration_must_match_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace_id, dataset_id, connection_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    workspace = Workspace(workspace_id=workspace_id, slug="tenant", name="Tenant", app_id="00042")
    dataset = ConnectorDataset(
        dataset_id=dataset_id, workspace_id=workspace_id, connection_id=connection_id,
        object_name="customers", active_mapping_version_id=uuid.uuid4(),
    )
    connection = ConnectorConnection(
        connection_id=connection_id, workspace_id=workspace_id, definition_key="postgresql",
        name="Customers", source_code="customers", source_system_id=11, status="READY",
    )

    class Repo:
        def __init__(self, session: object) -> None:
            pass

        async def get_workspace(self, _workspace: uuid.UUID):
            return workspace

        async def get_dataset(self, _workspace: uuid.UUID, _dataset: uuid.UUID):
            return dataset

        async def get_connection(self, _workspace: uuid.UUID, _connection: uuid.UUID):
            return connection

    class Session:
        def __init__(self) -> None:
            self.statements: list[str] = []

        async def get(self, _model: object, _key: uuid.UUID, **_kwargs: object):
            return workspace

        async def scalar(self, _statement: object):
            return None

        async def execute(self, statement: object, params: Any):
            self.statements.append(str(statement))
            assert params["source_code"] == f"dc1-{connection_id.hex}"
            return SimpleNamespace(scalar_one_or_none=lambda: None)

    monkeypatch.setattr(service, "ConnectorRepository", Repo)
    session = Session()
    with pytest.raises(service.ConnectorConflictError, match="source registration"):
        await service.materialize_sync_run(
            session, Settings(_env_file=None), workspace_id=workspace_id,
            dataset_id=dataset_id, trigger_type="MANUAL", idempotency_key="existing", commit=False,
        )
    assert len(session.statements) == 1
    assert "SELECT source_system_id FROM control.source_system" in session.statements[0]
