import uuid
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from app.main import create_app
from app.modules.connectors import controller
from app.modules.connectors.controller import _hide_expired_preview_result
from app.modules.connectors.model import ConnectorConnection, ConnectorOperation, SyncRun
from app.modules.connectors.schemas import (
    ConnectionCreate,
    DatasetPreviewRequest,
    DatasetUpdate,
    MappingCreate,
    OperationView,
    ScheduleUpsert,
    SourceCredentials,
)
from app.modules.connectors.service import (
    ConnectorConfigurationError,
    ConnectorConflictError,
    _physical_source_code,
    _validate_mapping_source_paths,
    queue_operation,
)
from app.workers.connectors import (
    _scheduler_bound,
    _scheduler_expression,
    _scheduler_start_bound,
)


def test_credentials_are_write_only_in_connection_contract() -> None:
    payload = ConnectionCreate(
        engine="postgresql",
        name="CRM source",
        source_code="crm_db",
        credentials=SourceCredentials(
            host="db.internal",
            port=5432,
            username="reader",
            password="do-not-render",
            database="crm",
            tunnel={
                "type": "ssh",
                "host": "bastion.example.com",
                "username": "ubuntu",
                "private_key": "private-key-material",
                "private_key_passphrase": "key-passphrase",
                "host_key": "ssh-ed25519 AAAA-test",
            },
        ),
    )
    secret = payload.credentials.secret_payload()
    assert secret["password"] == "do-not-render"
    assert secret["tunnel"]["private_key"] == "private-key-material"
    assert secret["tunnel"]["private_key_passphrase"] == "key-passphrase"
    assert "credentials" not in ConnectorConnection.__table__.columns


def test_ssh_tunnel_accepts_password_authentication() -> None:
    credentials = SourceCredentials(
        host="db.internal",
        port=5432,
        username="reader",
        password="database-password",
        database="crm",
        tunnel={
            "host": "bastion.example.com",
            "username": "ubuntu",
            "password": "ssh-password",
        },
    )

    secret = credentials.secret_payload()
    assert secret["tunnel"] == {
        "type": "ssh",
        "host": "bastion.example.com",
        "port": 22,
        "username": "ubuntu",
        "password": "ssh-password",
    }


@pytest.mark.parametrize(
    "tunnel",
    [
        {"host": "bastion.example.com", "username": "ubuntu"},
        {"host": "bastion.example.com", "username": "ubuntu", "password": ""},
        {
            "host": "bastion.example.com",
            "username": "ubuntu",
            "private_key": "key",
            "password": "password",
        },
        {
            "host": "bastion.example.com",
            "username": "ubuntu",
            "password": "password",
            "private_key_passphrase": "orphan-passphrase",
        },
    ],
)
def test_ssh_tunnel_rejects_invalid_authentication(tunnel: dict[str, str]) -> None:
    with pytest.raises(ValidationError):
        SourceCredentials(host="db.internal", tunnel=tunnel)


def test_ssh_tunnel_rejects_uri_and_multiple_database_hosts() -> None:
    tunnel = {
        "host": "bastion.example.com",
        "username": "ubuntu",
        "password": "ssh-password",
    }
    with pytest.raises(ValidationError, match="not uri"):
        SourceCredentials(uri="postgresql://db.example.com/crm", tunnel=tunnel)
    with pytest.raises(ValidationError, match="multiple database hosts"):
        SourceCredentials(hosts=["db-1.internal", "db-2.internal"], tunnel=tunnel)


@pytest.mark.asyncio
async def test_get_connection_by_id_is_workspace_scoped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    captured: dict[str, tuple[uuid.UUID, uuid.UUID]] = {}
    now = datetime.now(UTC)

    class FakeRepository:
        def __init__(self, _session: object) -> None:
            pass

        async def get_connection(
            self,
            requested_workspace_id: uuid.UUID,
            requested_connection_id: uuid.UUID,
        ) -> Any:
            captured["lookup"] = (requested_workspace_id, requested_connection_id)
            return SimpleNamespace(
                connection_id=connection_id,
                workspace_id=workspace_id,
                definition_key="postgresql",
                name="Customer PostgreSQL",
                source_code="customer_postgres",
                endpoint_label=None,
                source_system_id=None,
                safe_config={"display_color": "blue"},
                status="DRAFT",
                last_tested_at=None,
                last_error_code=None,
                created_at=now,
                updated_at=now,
            )

    monkeypatch.setattr(controller, "ConnectorRepository", FakeRepository)

    result = await controller.get_connection_by_id(
        workspace_id,
        connection_id,
        object(),  # type: ignore[arg-type]
    )

    assert captured["lookup"] == (workspace_id, connection_id)
    assert result.connection_id == connection_id
    assert result.safe_config == {"display_color": "blue"}


@pytest.mark.asyncio
async def test_get_connections_includes_record_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    now = datetime.now(UTC)

    async def fake_get_workspace(_session: object, requested_workspace_id: uuid.UUID) -> object:
        assert requested_workspace_id == workspace_id
        return object()

    class FakeRepository:
        def __init__(self, _session: object) -> None:
            pass

        async def list_connection_summaries(
            self, requested_workspace_id: uuid.UUID
        ) -> list[SimpleNamespace]:
            assert requested_workspace_id == workspace_id
            connection = SimpleNamespace(
                connection_id=connection_id,
                workspace_id=workspace_id,
                definition_key="postgresql",
                name="Customer PostgreSQL",
                source_code="customer_postgres",
                endpoint_label=None,
                source_system_id=2212,
                safe_config={},
                status="READY",
                last_tested_at=now,
                last_error_code=None,
                created_at=now,
                updated_at=now,
            )
            return [
                SimpleNamespace(
                    connection=connection,
                    successful_records=105_636,
                    error_records=4,
                    duplicate_records=423,
                    frequency="daily",
                )
            ]

    monkeypatch.setattr(controller, "get_workspace", fake_get_workspace)
    monkeypatch.setattr(controller, "ConnectorRepository", FakeRepository)

    result = await controller.get_connections(workspace_id, object())  # type: ignore[arg-type]

    assert result[0].successful_records == 105_636
    assert result[0].error_records == 4
    assert result[0].duplicate_records == 423
    assert result[0].frequency == "daily"


def test_sync_run_derives_duplicate_records() -> None:
    run = SyncRun(records_read=110, records_loaded=100, records_rejected=4)

    assert run.records_duplicate == 6


@pytest.mark.asyncio
async def test_get_connection_by_id_returns_404_when_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class MissingRepository:
        def __init__(self, _session: object) -> None:
            pass

        async def get_connection(
            self, _workspace_id: uuid.UUID, _connection_id: uuid.UUID
        ) -> None:
            return None

    monkeypatch.setattr(controller, "ConnectorRepository", MissingRepository)

    with pytest.raises(HTTPException) as exc_info:
        await controller.get_connection_by_id(
            uuid.uuid4(),
            uuid.uuid4(),
            object(),  # type: ignore[arg-type]
        )

    assert exc_info.value.status_code == 404


def test_mapping_requires_source_customer_id() -> None:
    with pytest.raises(ValidationError, match="source_customer_id"):
        MappingCreate(
            schema_snapshot_id=uuid.uuid4(),
            fields=[{"source_path": "email", "target_field": "email"}],
        )
    with pytest.raises(ValidationError, match="unsupported customer target_field"):
        MappingCreate(
            schema_snapshot_id=uuid.uuid4(),
            fields=[
                {"source_path": "id", "target_field": "source_customer_id"},
                {"source_path": "secret", "target_field": "password"},
            ],
        )


def test_mapping_source_paths_are_pinned_to_schema_snapshot() -> None:
    payload = MappingCreate(
        schema_snapshot_id=uuid.uuid4(),
        fields=[
            {"source_path": "customer_id", "target_field": "source_customer_id"},
            {"source_path": "profile.email", "target_field": "email"},
        ],
    )
    _validate_mapping_source_paths(
        {
            "columns": [
                {"name": "customer_id", "type": "text"},
                {"name": "profile", "type": "jsonb"},
            ]
        },
        payload,
    )
    invalid = payload.model_copy(
        update={
            "fields": [
                payload.fields[0],
                payload.fields[1].model_copy(update={"source_path": "missing.email"}),
            ]
        }
    )
    with pytest.raises(ConnectorConfigurationError, match="missing.email"):
        _validate_mapping_source_paths(
            {"columns": [{"name": "customer_id", "type": "text"}]}, invalid
        )

    with pytest.raises(ConnectorConfigurationError, match="profile.email"):
        _validate_mapping_source_paths(
            {
                "columns": [
                    {"name": "customer_id", "type": "text"},
                    {"name": "profile", "type": "mixed"},
                ]
            },
            payload,
        )


@pytest.mark.asyncio
async def test_preview_queues_workspace_scoped_idempotent_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace_id = uuid.uuid4()
    dataset_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    captured: dict[str, Any] = {}

    class FakeRepository:
        def __init__(self, _session: object) -> None:
            pass

        async def get_dataset(
            self, requested_workspace_id: uuid.UUID, requested_dataset_id: uuid.UUID
        ) -> Any:
            captured["lookup"] = (requested_workspace_id, requested_dataset_id)
            return SimpleNamespace(connection_id=connection_id)

    async def fake_queue_operation(_session: object, **kwargs: Any) -> Any:
        captured["queue"] = kwargs
        return SimpleNamespace(operation_id=uuid.uuid4(), status="QUEUED")

    monkeypatch.setattr(controller, "ConnectorRepository", FakeRepository)
    monkeypatch.setattr(controller, "queue_operation", fake_queue_operation)

    accepted = await controller.post_dataset_preview(
        workspace_id,
        dataset_id,
        controller.DatasetPreviewRequest(),
        object(),  # type: ignore[arg-type]
        "preview-key",
    )
    assert accepted.status == "QUEUED"
    assert captured["lookup"] == (workspace_id, dataset_id)
    assert captured["queue"] == {
        "workspace_id": workspace_id,
        "connection_id": connection_id,
        "dataset_id": dataset_id,
        "operation_type": "DATASET_PREVIEW",
        "idempotency_key": "preview-key",
        "request_json": {"limit": 10},
    }


@pytest.mark.asyncio
async def test_preview_rejects_dataset_from_another_workspace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class MissingRepository:
        def __init__(self, _session: object) -> None:
            pass

        async def get_dataset(self, _workspace_id: uuid.UUID, _dataset_id: uuid.UUID) -> None:
            return None

    monkeypatch.setattr(controller, "ConnectorRepository", MissingRepository)
    with pytest.raises(HTTPException) as exc_info:
        await controller.post_dataset_preview(
            uuid.uuid4(),
            uuid.uuid4(),
            controller.DatasetPreviewRequest(),
            object(),  # type: ignore[arg-type]
            None,
        )
    assert exc_info.value.status_code == 404


def test_preview_request_bounds_and_expired_results() -> None:
    assert DatasetPreviewRequest().limit == 10
    with pytest.raises(ValidationError):
        DatasetPreviewRequest(limit=51)

    now = datetime.now(UTC)
    view = OperationView(
        operation_id=uuid.uuid4(),
        workspace_id=uuid.uuid4(),
        connection_id=uuid.uuid4(),
        dataset_id=uuid.uuid4(),
        operation_type="DATASET_PREVIEW",
        trigger_type="MANUAL",
        status="SUCCEEDED",
        started_at=now,
        completed_at=now,
        error_code=None,
        error_message=None,
        result_json={"rows": [{"email": "raw@example.com"}]},
        result_expires_at=now - timedelta(seconds=1),
        created_at=now,
        updated_at=now,
    )
    assert _hide_expired_preview_result(view, now=now).result_json == {"expired": True}


@pytest.mark.asyncio
async def test_preview_idempotency_key_cannot_change_limit() -> None:
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    dataset_id = uuid.uuid4()
    connection = ConnectorConnection(
        connection_id=connection_id,
        workspace_id=workspace_id,
        definition_key="postgresql",
        name="CRM",
        source_code="crm",
        status="READY",
    )
    existing = ConnectorOperation(
        workspace_id=workspace_id,
        connection_id=connection_id,
        dataset_id=dataset_id,
        operation_type="DATASET_PREVIEW",
        trigger_type="MANUAL",
        idempotency_key="preview-key",
        request_json={"limit": 10},
    )

    class Session:
        def __init__(self) -> None:
            self.responses = [connection, existing]

        async def scalar(self, _statement: object) -> object:
            return self.responses.pop(0)

    with pytest.raises(ConnectorConflictError, match="another operation"):
        await queue_operation(  # type: ignore[arg-type]
            Session(),  # type: ignore[arg-type]
            workspace_id=workspace_id,
            connection_id=connection_id,
            dataset_id=dataset_id,
            operation_type="DATASET_PREVIEW",
            idempotency_key="preview-key",
            request_json={"limit": 20},
        )

def test_incremental_dataset_requires_cursor_and_primary_key() -> None:
    with pytest.raises(ValidationError, match="primary_key_paths"):
        DatasetUpdate(sync_mode="INCREMENTAL", cursor_paths=["updated_at"])
    payload = DatasetUpdate(
        sync_mode="INCREMENTAL",
        cursor_paths=["updated_at"],
        primary_key_paths=["id"],
    )
    assert payload.primary_key_paths == ["id"]


def test_recurring_schedule_validates_timezone_and_frequency() -> None:
    schedule = ScheduleUpsert(
        timing={
            "frequency": "weekly",
            "time": "09:15",
            "days_of_week": ["MON", "WED"],
        },
        timezone="Asia/Bangkok",
    )
    assert schedule.timezone == "Asia/Bangkok"
    assert _scheduler_expression(schedule.timing.model_dump()) == "cron(15 09 ? * MON,WED *)"

    with pytest.raises(ValidationError, match="unknown IANA timezone"):
        ScheduleUpsert(
            timing={"frequency": "daily", "time": "09:00"},
            timezone="Mars/Olympus",
        )

    assert _scheduler_bound(
        date(2026, 8, 29), "Asia/Bangkok", end_of_day=False
    ).isoformat() == "2026-08-28T17:00:00+00:00"


def test_scheduler_start_bound_uses_local_schedule_time() -> None:
    timing = {"frequency": "daily", "time": "10:30"}

    assert _scheduler_start_bound(
        date(2026, 9, 4),
        timing,
        "Asia/Bangkok",
        now=datetime(2026, 9, 4, 3, 24, tzinfo=UTC),
    ) == datetime(2026, 9, 4, 3, 30, tzinfo=UTC)


def test_scheduler_start_bound_omits_elapsed_time() -> None:
    assert _scheduler_start_bound(
        date(2026, 9, 4),
        {"frequency": "daily", "time": "10:30"},
        "Asia/Bangkok",
        now=datetime(2026, 9, 4, 3, 31, tzinfo=UTC),
    ) is None


def test_physical_source_code_is_workspace_scoped_and_bounded() -> None:
    workspace_id = uuid.UUID("00000000-0000-4000-8000-000000000001")
    result = _physical_source_code(workspace_id, "a" * 63)
    assert result.startswith("w00000000_")
    assert len(result) == 63


def test_active_sync_index_is_partial_and_unique() -> None:
    index = next(
        index
        for index in SyncRun.__table__.indexes
        if index.name == "uq_sync_run_active_dataset"
    )
    assert index.unique is True
    assert "QUEUED" in str(index.dialect_options["postgresql"]["where"])


def test_operation_type_constraint_allows_dataset_preview() -> None:
    constraint = next(
        constraint
        for constraint in ConnectorOperation.__table__.constraints
        if "operation_type IN" in str(getattr(constraint, "sqltext", ""))
    )
    assert "DATASET_PREVIEW" in str(constraint.sqltext)


def test_openapi_exposes_workspace_scoped_connector_routes(settings) -> None:
    paths = create_app(settings).openapi()["paths"]
    assert "/api/v1/workspaces/{workspace_id}/connectors" in paths
    assert (
        "/api/v1/workspaces/{workspace_id}/connectors/{connection_id}" in paths
    )
    assert (
        "/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/schema-snapshots"
        in paths
    )
    assert "/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/runs" in paths
    assert "/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/preview" in paths
