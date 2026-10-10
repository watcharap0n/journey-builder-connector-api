"""Real PostgreSQL + HTTP status projection, never mutate an external database.

CONNECTOR_PUBLICATION_TEST_URL must target local poststitch_status_test; run serially
with aws-flex-deploy's publication tests (both own only that disposable database).
"""

import os
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path

import asyncpg
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from app.core.config import Settings, get_settings
from app.infrastructure.database.base import Base
from app.infrastructure.database.session import get_db_session
from app.main import create_app
from app.modules.connectors.model import (
    ConnectorConnection,
    ConnectorDataset,
    ConnectorDefinition,
    ConnectorOperation,
    SyncRun,
    Workspace,
)

MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "aws-flex-deploy/database/migrations"
    / "025_customer_publication_status.sql"
)


@pytest.fixture
async def db() -> AsyncIterator[AsyncSession]:
    url = os.environ.get("CONNECTOR_PUBLICATION_TEST_URL")
    if not url:
        pytest.skip("Set CONNECTOR_PUBLICATION_TEST_URL to local poststitch_status_test")
    parsed = make_url(url)
    assert parsed.host in {"127.0.0.1", "localhost"}
    assert parsed.database == "poststitch_status_test"
    engine = create_async_engine(url)
    connection = await asyncpg.connect(
        host=parsed.host, port=parsed.port, user=parsed.username, database=parsed.database
    )
    try:
        await connection.execute("""
            DROP SCHEMA IF EXISTS connector CASCADE;
            DROP SCHEMA IF EXISTS platform CASCADE;
            DROP SCHEMA IF EXISTS control CASCADE;
            DROP SCHEMA IF EXISTS identity CASCADE;
            CREATE SCHEMA connector;
            CREATE SCHEMA platform;
            CREATE SCHEMA control;
            CREATE SCHEMA identity;
            CREATE TABLE control.ingestion_batch (
                batch_id uuid PRIMARY KEY, status text DEFAULT 'completed',
                records_rejected bigint DEFAULT 0);
            CREATE TABLE control.stitching_job (
                job_id uuid PRIMARY KEY, ingestion_batch_id uuid, attempt int,
                status text, created_at timestamptz DEFAULT now(),
                ready_record_count bigint DEFAULT 1, standardized_record_count bigint DEFAULT 1);
            CREATE TABLE identity.resolution_run (
                run_id uuid PRIMARY KEY, stitching_job_id uuid UNIQUE);
            CREATE TABLE control.customer_identity_transfer (
                transfer_id uuid DEFAULT gen_random_uuid(), ingestion_batch_id uuid,
                stitching_job_id uuid UNIQUE, resolution_run_id uuid UNIQUE,
                workspace_id uuid, app_id text, publication_seq bigint, status text,
                published_at timestamptz, error_code text, updated_at timestamptz DEFAULT now());
            CREATE TABLE control.customer_identity_capture_issue (
                stitching_job_id uuid PRIMARY KEY REFERENCES control.stitching_job(job_id),
                status text NOT NULL DEFAULT 'BLOCKED' CHECK (status='BLOCKED'),
                error_code text NOT NULL,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp());
            CREATE VIEW control.customer_pipeline_status AS
                SELECT b.batch_id AS ingestion_batch_id, j.job_id AS stitching_job_id,
                       r.run_id AS resolution_run_id, 'COMPLETED'::text AS overall_status,
                       now() AS updated_at
                FROM control.ingestion_batch b
                JOIN control.stitching_job j ON j.ingestion_batch_id=b.batch_id
                JOIN identity.resolution_run r ON r.stitching_job_id=j.job_id;
        """)
        await connection.execute(MIGRATION.read_text())
    finally:
        await connection.close()
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda c: Base.metadata.create_all(
                c,
                tables=[
                    t for t in Base.metadata.sorted_tables if t.schema in {"connector", "platform"}
                ],
            )
        )
    async with AsyncSession(engine, expire_on_commit=False) as session:
        yield session
    await engine.dispose()


async def seed(db: AsyncSession, *, operation_type: str = "DATASET_SYNC") -> SyncRun:
    workspace = Workspace(slug=uuid.uuid4().hex, name="publication test")
    db.add(workspace)
    definition = await db.get(ConnectorDefinition, "test")
    if definition is None:
        definition = ConnectorDefinition(key="test", family="db", adapter_key="test")
        db.add(definition)
    await db.flush()
    connection = ConnectorConnection(
        workspace_id=workspace.workspace_id,
        definition_key="test",
        name="test",
        source_code="test",
        status="READY",
    )
    db.add(connection)
    await db.flush()
    dataset = ConnectorDataset(
        workspace_id=workspace.workspace_id,
        connection_id=connection.connection_id,
        object_name="customers",
    )
    db.add(dataset)
    await db.flush()
    operation = ConnectorOperation(
        workspace_id=workspace.workspace_id,
        connection_id=connection.connection_id,
        dataset_id=dataset.dataset_id,
        operation_type=operation_type,
        status="SUCCEEDED",
        idempotency_key=uuid.uuid4().hex,
    )
    db.add(operation)
    await db.flush()
    run = SyncRun(
        operation_id=operation.operation_id,
        workspace_id=workspace.workspace_id,
        dataset_id=dataset.dataset_id,
        status="SUCCEEDED",
        landing_batch_id=uuid.uuid4(),
        ingestion_batch_id=uuid.uuid4(),
        stitching_job_id=uuid.uuid4(),
        mapping_version_id=uuid.uuid4(),
        records_loaded=7,
        records_read=10,
        records_rejected=1,
        created_at=datetime.now(UTC),
    )
    db.add(run)
    await db.execute(
        text("INSERT INTO control.ingestion_batch(batch_id) VALUES (:batch)"),
        {"batch": run.ingestion_batch_id},
    )
    await db.execute(
        text("""INSERT INTO control.stitching_job
        (job_id,ingestion_batch_id,attempt,status) VALUES (:job,:batch,1,'SUCCEEDED')"""),
        {"job": run.stitching_job_id, "batch": run.ingestion_batch_id},
    )
    await db.execute(
        text("INSERT INTO identity.resolution_run VALUES (:resolution,:job)"),
        {"resolution": uuid.uuid4(), "job": run.stitching_job_id},
    )
    await db.commit()
    return run


async def request(
    db: AsyncSession,
    path: str,
    *,
    enabled: bool = True,
    method: str = "GET",
    idempotency_key: str | None = None,
):
    settings = Settings(_env_file=None, connector_publication_status_enabled=enabled)
    app = create_app(settings)
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_db_session] = lambda: db
    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False), base_url="http://test"
    ) as client:
        return await client.request(
            method,
            "/api/v1" + path,
            headers={"Idempotency-Key": idempotency_key} if idempotency_key else {},
        )


@pytest.mark.parametrize("enabled", [False, True])
async def test_idempotent_post_uses_same_publication_projection_as_get(db: AsyncSession, enabled):
    run = await seed(db)
    operation = await db.get(ConnectorOperation, run.operation_id)
    dataset = await db.get(ConnectorDataset, run.dataset_id)
    dataset.active_mapping_version_id = run.mapping_version_id
    await db.commit()
    response = await request(
        db,
        f"/workspaces/{run.workspace_id}/datasets/{run.dataset_id}/runs",
        enabled=enabled,
        method="POST",
        idempotency_key=operation.idempotency_key,
    )
    assert response.status_code == 202
    assert response.json()["sync_run_id"] == str(run.sync_run_id)
    assert response.json()["status"] == ("TRANSFER_PENDING" if enabled else "SUCCEEDED")
    get_response = await request(
        db, f"/workspaces/{run.workspace_id}/runs/{run.sync_run_id}", enabled=enabled
    )
    assert response.json() == get_response.json()
    assert await db.scalar(text("SELECT count(*) FROM connector.sync_run")) == 1
    assert await db.scalar(text("SELECT count(*) FROM connector.operation")) == 1
    assert run.status == operation.status == "SUCCEEDED"
    assert not db.dirty


async def test_operation_success_requires_published_transfer(db: AsyncSession):
    run = await seed(db)
    response = await request(db, f"/workspaces/{run.workspace_id}/operations/{run.operation_id}")
    assert response.status_code == 200
    assert response.json()["status"] == "TRANSFER_PENDING"
    operation = await db.get(ConnectorOperation, run.operation_id)
    assert operation.status == "SUCCEEDED"
    assert not db.dirty


async def test_sync_run_success_requires_published_transfer(db: AsyncSession):
    run = await seed(db)
    response = await request(db, f"/workspaces/{run.workspace_id}/runs/{run.sync_run_id}")
    assert response.status_code == 200
    assert response.json()["status"] == "TRANSFER_PENDING"
    assert run.status == "SUCCEEDED"
    assert not db.dirty


async def test_unpublished_records_are_not_successful_in_bulk_listing(db: AsyncSession):
    run = await seed(db)
    response = await request(db, f"/workspaces/{run.workspace_id}/connectors")
    assert response.status_code == 200
    summary = response.json()[0]
    assert summary["successful_records"] == 0
    assert summary["error_records"] == 1
    assert summary["duplicate_records"] == 2


async def transfer(db: AsyncSession, run: SyncRun, status: str, *, workspace_id=None):
    await db.execute(
        text("""INSERT INTO control.customer_identity_transfer
        (ingestion_batch_id,stitching_job_id,resolution_run_id,workspace_id,status)
        SELECT :batch,j.job_id,r.run_id,:workspace,:status
        FROM control.stitching_job j JOIN identity.resolution_run r ON r.stitching_job_id=j.job_id
        WHERE j.ingestion_batch_id=:batch ORDER BY j.attempt DESC LIMIT 1"""),
        {
            "batch": run.ingestion_batch_id,
            "workspace": workspace_id or run.workspace_id,
            "status": status,
        },
    )
    await db.commit()


@pytest.mark.parametrize(
    "status,expected",
    [
        ("PENDING", "TRANSFER_PENDING"),
        ("RUNNING", "TRANSFERRING"),
        ("FAILED", "TRANSFER_FAILED"),
        ("BLOCKED", "TRANSFER_BLOCKED"),
        ("PUBLISHED", "COMPLETED"),
    ],
)
@pytest.mark.parametrize("route", ["runs", "operations"])
async def test_http_transfer_statuses(db: AsyncSession, status: str, expected: str, route: str):
    run = await seed(db)
    await transfer(db, run, status)
    key = run.sync_run_id if route == "runs" else run.operation_id
    response = await request(db, f"/workspaces/{run.workspace_id}/{route}/{key}")
    assert response.status_code == 200
    assert response.json()["status"] == expected
    assert run.status == "SUCCEEDED"
    assert not db.dirty


async def test_publication_workspace_must_match_even_when_null(db: AsyncSession):
    run = await seed(db)
    await transfer(db, run, "PUBLISHED")
    await db.execute(text("UPDATE control.customer_identity_transfer SET workspace_id=NULL"))
    await db.commit()
    response = await request(db, f"/workspaces/{run.workspace_id}/runs/{run.sync_run_id}")
    assert response.json()["status"] == "TRANSFER_BLOCKED"


@pytest.mark.parametrize("route", ["operations", "runs", "connectors", "post"])
@pytest.mark.parametrize("enabled", [False, True])
async def test_cutover_missing_schema_never_silently_succeeds(db: AsyncSession, route, enabled):
    run = await seed(db)
    operation = await db.get(ConnectorOperation, run.operation_id)
    dataset = await db.get(ConnectorDataset, run.dataset_id)
    dataset.active_mapping_version_id = run.mapping_version_id
    await db.execute(text("DROP VIEW control.customer_publication_status CASCADE"))
    await db.commit()
    suffix = {
        "operations": f"operations/{run.operation_id}",
        "runs": f"runs/{run.sync_run_id}",
        "connectors": "connectors",
        "post": f"datasets/{run.dataset_id}/runs",
    }[route]
    response = await request(
        db,
        f"/workspaces/{run.workspace_id}/{suffix}",
        enabled=enabled,
        method="POST" if route == "post" else "GET",
        idempotency_key=operation.idempotency_key,
    )
    if enabled:
        assert response.status_code == 500
    else:
        assert response.status_code == (202 if route == "post" else 200)
        if route == "connectors":
            assert response.json()[0]["successful_records"] == 7
        else:
            assert response.json()["status"] == "SUCCEEDED"


@pytest.mark.parametrize("kind", ["DATASET_PREVIEW", "CONNECTION_TEST", "SCHEMA_DISCOVERY"])
async def test_non_sync_operations_ignore_publication_schema(db: AsyncSession, kind):
    run = await seed(db, operation_type=kind)
    operation = await db.get(ConnectorOperation, run.operation_id)
    await db.execute(text("DROP VIEW control.customer_publication_status CASCADE"))
    await db.commit()
    response = await request(db, f"/workspaces/{run.workspace_id}/operations/{run.operation_id}")
    assert response.status_code == 200
    assert response.json()["status"] == "SUCCEEDED"
    assert datetime.fromisoformat(response.json()["updated_at"]) == operation.updated_at


async def test_retry_publication_follows_batch_not_original_connector_job(db: AsyncSession):
    run = await seed(db)
    await transfer(db, run, "PUBLISHED")
    retry = uuid.uuid4()
    await db.execute(
        text("""INSERT INTO control.stitching_job
        (job_id,ingestion_batch_id,attempt,status) VALUES (:job,:batch,2,'SUCCEEDED')"""),
        {"job": retry, "batch": run.ingestion_batch_id},
    )
    await db.execute(
        text("INSERT INTO identity.resolution_run VALUES (:id,:job)"),
        {"id": uuid.uuid4(), "job": retry},
    )
    await db.commit()
    route = f"/workspaces/{run.workspace_id}/runs/{run.sync_run_id}"
    assert (await request(db, route)).json()["status"] == "TRANSFER_PENDING"
    await transfer(db, run, "PUBLISHED")
    assert (await request(db, route)).json()["status"] == "COMPLETED"
    assert run.stitching_job_id != retry
    assert not db.dirty


@pytest.mark.parametrize("route", ["runs", "operations", "post", "connectors"])
@pytest.mark.parametrize("execution_status", ["FAILED", "CANCELLED", "SKIPPED"])
async def test_failed_original_follows_effective_retry_without_rewriting_truth(
    db: AsyncSession, route, execution_status: str
):
    run = await seed(db)
    operation = await db.get(ConnectorOperation, run.operation_id)
    dataset = await db.get(ConnectorDataset, run.dataset_id)
    dataset.active_mapping_version_id = run.mapping_version_id
    run.status = operation.status = execution_status
    operation.error_code = "STITCHING_FAILED"
    await db.execute(text("UPDATE control.stitching_job SET status='FAILED'"))
    retry = uuid.uuid4()
    await db.execute(
        text("""INSERT INTO control.stitching_job
        (job_id,ingestion_batch_id,attempt,status) VALUES (:job,:batch,2,'SUCCEEDED')"""),
        {"job": retry, "batch": run.ingestion_batch_id},
    )
    await db.execute(
        text("INSERT INTO identity.resolution_run VALUES (:id,:job)"),
        {"id": uuid.uuid4(), "job": retry},
    )
    await db.commit()
    for published in (False, True):
        if published:
            await transfer(db, run, "PUBLISHED")
        suffix = {
            "runs": f"runs/{run.sync_run_id}",
            "operations": f"operations/{run.operation_id}",
            "post": f"datasets/{run.dataset_id}/runs",
            "connectors": "connectors",
        }[route]
        response = await request(
            db,
            f"/workspaces/{run.workspace_id}/{suffix}",
            method="POST" if route == "post" else "GET",
            idempotency_key=operation.idempotency_key,
        )
        assert response.status_code == (202 if route == "post" else 200)
        if route == "connectors":
            assert response.json()[0]["successful_records"] == (7 if published else 0)
            assert response.json()[0]["error_records"] == 0
            assert response.json()[0]["duplicate_records"] == 0
        else:
            assert response.json()["status"] == ("COMPLETED" if published else "TRANSFER_PENDING")
    assert not db.dirty
    await db.refresh(run)
    await db.refresh(operation)
    assert run.status == operation.status == execution_status
    assert operation.error_code == "STITCHING_FAILED"
    assert run.stitching_job_id != retry


@pytest.mark.parametrize(
    "stitching_status,expected",
    [
        ("QUEUED", "STITCHING_QUEUED"),
        ("WAITING_FOR_LOCK", "WAITING_FOR_LOCK"),
        ("VALIDATING", "VALIDATING"),
        ("RUNNING", "STITCHING"),
        ("FAILED", "FAILED"),
    ],
)
async def test_effective_post_ingestion_stage_overrides_stale_execution(
    db: AsyncSession, stitching_status: str, expected: str
):
    run = await seed(db)
    operation = await db.get(ConnectorOperation, run.operation_id)
    run.status = "STITCHING"
    operation.status = "RUNNING"
    await db.execute(
        text("UPDATE control.stitching_job SET status=:status"), {"status": stitching_status}
    )
    await db.commit()
    for route, key in [("runs", run.sync_run_id), ("operations", run.operation_id)]:
        response = await request(db, f"/workspaces/{run.workspace_id}/{route}/{key}")
        assert response.status_code == 200
        assert response.json()["status"] == expected
    assert not db.dirty


@pytest.mark.parametrize("ingestion_status", ["queued", "processing", "failed", "completed"])
async def test_pre_stitch_failure_is_not_hidden_by_precreated_lineage(
    db: AsyncSession, ingestion_status: str
):
    run = await seed(db)
    operation = await db.get(ConnectorOperation, run.operation_id)
    run.status = operation.status = "FAILED"
    operation.error_code = "SOURCE_READ_FAILED"
    await db.execute(
        text("UPDATE control.ingestion_batch SET status=:status"), {"status": ingestion_status}
    )
    await db.execute(text("DELETE FROM identity.resolution_run"))
    if ingestion_status == "completed":
        await db.execute(text("DELETE FROM control.stitching_job"))
    else:
        await db.execute(text("UPDATE control.stitching_job SET status='QUEUED'"))
    await db.commit()
    for route, key in [("runs", run.sync_run_id), ("operations", run.operation_id)]:
        response = await request(db, f"/workspaces/{run.workspace_id}/{route}/{key}")
        assert response.status_code == 200
        assert response.json()["status"] == "FAILED"
    summary = await request(db, f"/workspaces/{run.workspace_id}/connectors")
    assert summary.status_code == 200
    assert summary.json()[0]["successful_records"] == 0
    assert not db.dirty


@pytest.mark.parametrize("execution_status", ["SUCCEEDED", "FAILED"])
async def test_bulk_totals_published_only_without_n_plus_one(db: AsyncSession, execution_status):
    run = await seed(db)
    operation = await db.get(ConnectorOperation, run.operation_id)
    run.status = operation.status = execution_status
    # Independent connections in one workspace; include stale failed executions.

    await transfer(db, run, "PUBLISHED")
    for i, transfer_status in enumerate(["FAILED", "BLOCKED", "RUNNING", "PENDING", "PUBLISHED"]):
        conn = ConnectorConnection(
            workspace_id=run.workspace_id,
            definition_key="test",
            name=f"connection-{i}",
            source_code=f"source-{i}",
            status="READY",
        )
        db.add(conn)
        await db.flush()
        ds = ConnectorDataset(
            workspace_id=run.workspace_id, connection_id=conn.connection_id, object_name="customers"
        )
        db.add(ds)
        await db.flush()
        op = ConnectorOperation(
            workspace_id=run.workspace_id,
            connection_id=conn.connection_id,
            dataset_id=ds.dataset_id,
            operation_type="DATASET_SYNC",
            status=execution_status,
            idempotency_key=uuid.uuid4().hex,
        )
        db.add(op)
        await db.flush()
        other = SyncRun(
            operation_id=op.operation_id,
            workspace_id=run.workspace_id,
            dataset_id=ds.dataset_id,
            landing_batch_id=uuid.uuid4(),
            ingestion_batch_id=uuid.uuid4(),
            stitching_job_id=uuid.uuid4(),
            mapping_version_id=uuid.uuid4(),
            status=execution_status,
            records_loaded=7,
            records_read=10,
            records_rejected=1,
            created_at=datetime.now(UTC),
        )
        db.add(other)
        await db.flush()
        await db.execute(
            text("INSERT INTO control.ingestion_batch(batch_id) VALUES (:id)"),
            {"id": other.ingestion_batch_id},
        )
        await db.execute(
            text("""INSERT INTO control.stitching_job
            (job_id,ingestion_batch_id,attempt,status) VALUES (:job,:batch,1,'SUCCEEDED')"""),
            {"job": other.stitching_job_id, "batch": other.ingestion_batch_id},
        )
        await db.execute(
            text("INSERT INTO identity.resolution_run VALUES (:id,:job)"),
            {"id": uuid.uuid4(), "job": other.stitching_job_id},
        )
        await transfer(db, other, transfer_status)
    queries = []

    def listener(*args):
        queries.append(args[2])

    event.listen(db.bind.sync_engine, "before_cursor_execute", listener)
    try:
        response = await request(db, f"/workspaces/{run.workspace_id}/connectors")
    finally:
        event.remove(db.bind.sync_engine, "before_cursor_execute", listener)
    assert response.status_code == 200
    assert len(response.json()) == 6
    assert sum(r["successful_records"] for r in response.json()) == 14
    assert sum(r["error_records"] for r in response.json()) == (
        6 if execution_status == "SUCCEEDED" else 0
    )
    assert sum(r["duplicate_records"] for r in response.json()) == (
        12 if execution_status == "SUCCEEDED" else 0
    )
    assert len(queries) <= 4  # workspace, connections, one aggregate, schedules
    assert not db.dirty


@pytest.mark.parametrize("route", ["runs", "operations"])
@pytest.mark.parametrize("state", ["FAILED", "CANCELLED", "QUEUED", "SUCCEEDED"])
async def test_execution_states_and_missing_lineage_are_safe(db: AsyncSession, route, state):
    run = await seed(db)
    operation = await db.get(ConnectorOperation, run.operation_id)
    operation.status = state
    run.status = state
    await db.execute(text("DELETE FROM identity.resolution_run"))
    await db.execute(text("DELETE FROM control.stitching_job"))
    await db.execute(text("DELETE FROM control.ingestion_batch"))
    await db.commit()
    key = run.sync_run_id if route == "runs" else run.operation_id
    response = await request(db, f"/workspaces/{run.workspace_id}/{route}/{key}")
    assert response.status_code == 200
    assert response.json()["status"] == ("TRANSFER_PENDING" if state == "SUCCEEDED" else state)
    assert not db.dirty


async def test_wrong_workspace_is_not_found(db: AsyncSession):
    run = await seed(db)
    for route, key in [("runs", run.sync_run_id), ("operations", run.operation_id)]:
        response = await request(db, f"/workspaces/{uuid.uuid4()}/{route}/{key}")
        assert response.status_code == 404


async def test_published_warning_status_is_projected(db: AsyncSession):
    run = await seed(db)
    await db.execute(text("UPDATE control.ingestion_batch SET records_rejected=1"))
    await transfer(db, run, "PUBLISHED")
    response = await request(db, f"/workspaces/{run.workspace_id}/runs/{run.sync_run_id}")
    assert response.json()["status"] == "COMPLETED_WITH_WARNINGS"


@pytest.mark.parametrize(
    "has_resolution,error_code",
    [
        (False, "NO_SUCCESSFUL_RESOLUTION"),
        (True, "CAPTURE_SQL_ERROR"),
        (True, "CAPTURE_LOCK_CONTENDED"),
    ],
)
async def test_capture_issue_projects_block_reason_and_clock_without_mutating_execution(
    db: AsyncSession, has_resolution: bool, error_code: str
):
    run = await seed(db)
    operation = await db.get(ConnectorOperation, run.operation_id)
    dataset = await db.get(ConnectorDataset, run.dataset_id)
    dataset.active_mapping_version_id = run.mapping_version_id
    issue_at = datetime(2099, 1, 1, tzinfo=UTC)
    if not has_resolution:
        await db.execute(text("DELETE FROM identity.resolution_run"))
    await db.execute(
        text("""INSERT INTO control.customer_identity_capture_issue
            (stitching_job_id,error_code,created_at) VALUES (:job,:error,:created)"""),
        {
            "job": run.stitching_job_id,
            "error": error_code,
            "created": issue_at,
        },
    )
    await db.commit()
    for route in ("runs", "operations", "post"):
        suffix = {
            "runs": f"runs/{run.sync_run_id}",
            "operations": f"operations/{run.operation_id}",
            "post": f"datasets/{run.dataset_id}/runs",
        }[route]
        response = await request(
            db,
            f"/workspaces/{run.workspace_id}/{suffix}",
            method="POST" if route == "post" else "GET",
            idempotency_key=operation.idempotency_key,
        )
        assert response.status_code == (202 if route == "post" else 200)
        body = response.json()
        assert body["status"] == "TRANSFER_BLOCKED"
        assert body.get("transfer_error_code") == error_code
        if route == "operations":
            assert datetime.fromisoformat(body["updated_at"]) == issue_at
            assert body["error_code"] is None
    summary = await request(db, f"/workspaces/{run.workspace_id}/connectors")
    assert summary.json()[0]["successful_records"] == 0
    assert not db.dirty
    await db.refresh(operation)
    assert operation.status == run.status == "SUCCEEDED"
    assert operation.error_code is None


@pytest.mark.parametrize("transfer_is_newer", [False, True])
@pytest.mark.parametrize("transfer_status", ["RUNNING", "FAILED", "PUBLISHED"])
async def test_operation_updated_at_tracks_transfer_not_execution_completion(
    db: AsyncSession, transfer_is_newer: bool, transfer_status: str
):
    run = await seed(db)
    operation = await db.get(ConnectorOperation, run.operation_id)
    completed = datetime(2025, 1, 1, tzinfo=UTC)
    operation_updated = datetime(2025, 1, 3, tzinfo=UTC)
    transfer_updated = datetime(2025, 1, 4 if transfer_is_newer else 2, tzinfo=UTC)
    operation.completed_at = run.completed_at = completed
    operation.updated_at = operation_updated
    await transfer(db, run, transfer_status)
    await db.execute(
        text("UPDATE control.customer_identity_transfer SET updated_at=:updated"),
        {"updated": transfer_updated},
    )
    await db.commit()
    response = await request(db, f"/workspaces/{run.workspace_id}/operations/{run.operation_id}")
    assert response.status_code == 200
    assert response.json()["status"] == {
        "RUNNING": "TRANSFERRING", "FAILED": "TRANSFER_FAILED", "PUBLISHED": "COMPLETED"
    }[transfer_status]
    assert datetime.fromisoformat(response.json()["updated_at"]) == max(
        operation_updated, transfer_updated
    )
    assert datetime.fromisoformat(response.json()["completed_at"]) == completed
    assert not db.dirty
    await db.refresh(operation)
    assert operation.updated_at == operation_updated
    assert operation.status == "SUCCEEDED"


def test_cutover_flag_default_off_and_explicit_environment_activation(monkeypatch):
    monkeypatch.delenv("CONNECTOR_PUBLICATION_STATUS_ENABLED", raising=False)
    assert Settings(_env_file=None).connector_publication_status_enabled is False
    monkeypatch.setenv("CONNECTOR_PUBLICATION_STATUS_ENABLED", "true")
    assert Settings(_env_file=None).connector_publication_status_enabled is True
