import uuid
from datetime import date

import pytest
from pydantic import ValidationError

from app.main import create_app
from app.modules.connectors.model import ConnectorConnection, SyncRun
from app.modules.connectors.schemas import (
    ConnectionCreate,
    DatasetUpdate,
    MappingCreate,
    ScheduleUpsert,
    SourceCredentials,
)
from app.modules.connectors.service import _physical_source_code
from app.workers.connectors import _scheduler_bound, _scheduler_expression


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
        ),
    )
    secret = payload.credentials.secret_payload()
    assert secret["password"] == "do-not-render"
    assert "credentials" not in ConnectorConnection.__table__.columns


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


def test_openapi_exposes_workspace_scoped_connector_routes(settings) -> None:
    paths = create_app(settings).openapi()["paths"]
    assert "/api/v1/workspaces/{workspace_id}/connectors" in paths
    assert "/api/v1/workspaces/{workspace_id}/datasets/{dataset_id}/runs" in paths
