"""Opt-in real API -> sync lineage -> raw/staging on an owned local database.

Uses pinned CDP SQL/connector DDL, no deployment or shared database writes.
"""

from __future__ import annotations

import ast
import asyncio
import importlib.util
import os
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast

import asyncpg  # type: ignore[import-untyped]
import pytest
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from app.core.config import Settings
from app.infrastructure.database.base import Base
from app.infrastructure.database.session import get_db_session
from app.main import create_app
from app.modules.connectors.model import (
    ConnectorConnection,
    ConnectorDataset,
    MappingVersion,
    SchemaSnapshot,
)
from app.modules.connectors.service import materialize_sync_run

pytestmark = pytest.mark.skipif(
    os.getenv("CDP_CONNECTOR_APP_PG_TESTS") != "1", reason="opt-in local PostgreSQL"
)
API = Path(__file__).resolve().parents[1]
PROJECTS = API.parent
CDP = PROJECTS / "aws-flex-deploy"
CATALOG = PROJECTS / "fargate-user-stitching"
PORT = 55441


def _assigned_sql(name: str, key: str) -> str:
    tree = ast.parse((API / "alembic/versions" / name).read_text())
    return cast(str, next(
        ast.literal_eval(node.value)
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == key for target in node.targets)
    ))


@pytest.fixture
async def db() -> AsyncIterator[tuple[AsyncEngine, AsyncSession]]:
    admin = await asyncpg.connect(host="127.0.0.1", port=PORT, user="postgres", database="postgres")
    database = "cdp_connector_e2e_" + uuid.uuid4().hex
    api_role = "ca_" + uuid.uuid4().hex
    worker_role = "cw_" + uuid.uuid4().hex
    assert await admin.fetchval("SELECT pg_is_in_recovery()") is False
    assert not await admin.fetchval("SELECT 1 FROM pg_database WHERE datname=$1", database)
    created = False
    engine: AsyncEngine | None = None
    try:
        for role in (api_role, worker_role):
            await admin.execute(f'CREATE ROLE "{role}" NOLOGIN')
        await admin.execute(f"CREATE DATABASE \"{database}\" TEMPLATE template0 ENCODING 'UTF8'")
        created = True
        raw = await asyncpg.connect(host="127.0.0.1", port=PORT, user="postgres", database=database)
        try:
            for path in (
                CATALOG / "tests/fixtures/repository_catalog.sql",
                CATALOG / "src/identity_stitcher/sql/001_identity_model.sql",
                CDP / "database/migrations/014_pipeline_tracking.sql",
                CDP / "database/migrations/024_customer_identity_publication.sql",
            ):
                await raw.execute(path.read_text())
            typed = (CDP / "database/migrations/019_customer_typed_history.sql").read_text()
            await raw.execute(
                typed[
                    typed.index("CREATE FUNCTION raw.capture_customer_history()") : typed.index(
                        "-- Batch creation order"
                    )
                ]
            )
            for filename in ("022_customer_bulk_ingestion.sql", "026_customer_app_scope.sql"):
                await raw.execute((CDP / "database/migrations" / filename).read_text())
            await raw.execute(_assigned_sql("001_connector_control_plane.py", "DDL_STATEMENTS"))
            await raw.execute(_assigned_sql("003_connector_dataset_preview.py", "UPGRADE_DDL"))
            await raw.execute("SELECT set_config('cdp.connector_api_role',$1,false)", api_role)
            await raw.execute(
                "SELECT set_config('cdp.connector_worker_role',$1,false)", worker_role
            )
            # The first sync must only run after both 005 and 040 are present.
            engine = create_async_engine(
                f"postgresql+asyncpg://postgres@127.0.0.1:{PORT}/{database}"
            )
            for filename in ("004_connector_operation_leases.py", "005_workspace_app_id.py"):
                spec = importlib.util.spec_from_file_location(
                    filename.replace(".py", ""), API / "alembic/versions" / filename
                )
                assert spec and spec.loader
                revision = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(revision)
                async with engine.begin() as connection:

                    def migrate(
                        sync_connection: Connection, revision: Any = revision
                    ) -> None:
                        context = MigrationContext.configure(
                            sync_connection, opts={"target_metadata": Base.metadata}
                        )
                        with Operations.context(context):
                            revision.upgrade()

                    await connection.run_sync(migrate)
            await raw.execute(
                (CDP / "database/migrations/040_connector_app_lineage.sql").read_text()
            )
        finally:
            await raw.close()
        async with AsyncSession(engine, expire_on_commit=False) as session:
            yield engine, session
    finally:
        if engine is not None:
            await engine.dispose()
        if created:
            assert await admin.fetchval("SELECT 1 FROM pg_database WHERE datname=$1", database)
            await admin.execute(f'DROP DATABASE "{database}"')
            assert not await admin.fetchval("SELECT 1 FROM pg_database WHERE datname=$1", database)
        for role in (api_role, worker_role):
            if await admin.fetchval("SELECT 1 FROM pg_roles WHERE rolname=$1", role):
                await admin.execute(f'DROP ROLE "{role}"')
        await admin.close()


async def test_http_workspace_sync_and_raw_staging_preserve_literal_app(
    db: tuple[AsyncEngine, AsyncSession], tmp_path: Path,
) -> None:
    engine, session = db
    app = create_app()
    app.dependency_overrides[get_db_session] = lambda: session
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/v1/workspaces",
            json={"slug": "literal-app", "name": "Literal app", "app_id": "00042"},
        )
        assert response.status_code == 201, response.text
        workspace_id = uuid.UUID(response.json()["workspace_id"])
        fetched = await client.get(f"/api/v1/workspaces/{workspace_id}")
        assert fetched.json()["app_id"] == "00042"

    connection = ConnectorConnection(
        workspace_id=workspace_id,
        definition_key="postgresql",
        name="Customers",
        source_code="customers",
        status="READY",
    )
    session.add(connection)
    await session.flush()
    dataset = ConnectorDataset(
        workspace_id=workspace_id,
        connection_id=connection.connection_id,
        object_name="customers",
        status="ACTIVE",
    )
    session.add(dataset)
    await session.flush()
    snapshot = SchemaSnapshot(
        workspace_id=workspace_id,
        dataset_id=dataset.dataset_id,
        schema_hash="0" * 64,
        schema_json={"columns": []},
    )
    session.add(snapshot)
    await session.flush()
    mapping = MappingVersion(
        workspace_id=workspace_id,
        dataset_id=dataset.dataset_id,
        version=1,
        schema_snapshot_id=snapshot.schema_snapshot_id,
        mapping_json={},
        mapping_hash="1" * 64,
        status="PUBLISHED",
    )
    session.add(mapping)
    await session.flush()
    dataset.active_mapping_version_id = mapping.mapping_version_id
    await session.commit()

    operation, run = await materialize_sync_run(
        session,
        Settings(_env_file=None),
        workspace_id=workspace_id,
        dataset_id=dataset.dataset_id,
        trigger_type="MANUAL",
        idempotency_key="first",
        commit=True,
    )
    assert operation.workspace_id == run.workspace_id == workspace_id
    assert (
        await session.scalar(
            text("SELECT app_id FROM control.ingestion_batch WHERE batch_id=:id"),
            {"id": run.ingestion_batch_id},
        )
        == "00042"
    )
    assert (
        await session.scalar(
            text("SELECT source_code FROM control.source_system WHERE source_system_id=:id"),
            {"id": connection.source_system_id},
        )
        == "dc1-" + connection.connection_id.hex
    )
    assert (
        await session.scalar(
            text("SELECT count(*) FROM connector.outbox_event WHERE aggregate_id=:id"),
            {"id": operation.operation_id},
        )
        == 1
    )
    assert (
        await session.execute(
            text("SELECT * FROM control.customer_app_scope_status(:id)"),
            {"id": run.ingestion_batch_id},
        )
    ).one() == (None, "CONNECTOR_EXCLUDED")
    await session.commit()
    # Run the actual connector Parquet loader in its own dependency environment
    # against this exact API-created batch, not a synthetic worker scope function.
    await session.execute(
        text("UPDATE connector.sync_run SET status='PUBLISHING' WHERE sync_run_id=:id"),
        {"id": run.sync_run_id},
    )
    await session.commit()
    worker = PROJECTS / "cdp-database-connector-worker"
    probe = await asyncio.create_subprocess_exec(
        str(worker / ".venv/bin/python"),
        str(worker / "tests/connector_app_pipeline_probe.py"),
        f"host=127.0.0.1 port={PORT} user=postgres dbname={engine.url.database}",
        str(operation.operation_id), str(workspace_id), str(connection.connection_id),
        str(dataset.dataset_id), str(mapping.mapping_version_id), str(run.sync_run_id),
        str(run.landing_batch_id), str(run.ingestion_batch_id), str(run.stitching_job_id),
        str(connection.source_system_id), str(tmp_path), cwd=str(worker),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(probe.communicate(), timeout=60)
    assert probe.returncode == 0, stdout.decode() + stderr.decode()
    assert "loaded=1" in stdout.decode()
    assert await session.scalar(text("""
        SELECT app_id FROM raw.customer_current
        WHERE source_system_id=:source AND source_customer_id='worker-key'
    """), {"source": connection.source_system_id}) == "00042"
    assert await session.scalar(text("""
        SELECT app_id FROM raw.customer WHERE batch_id=:batch AND source_customer_id='worker-key'
    """), {"batch": run.ingestion_batch_id}) == "00042"
    assert await session.scalar(text("""
        SELECT status FROM connector.sync_run WHERE sync_run_id=:id
    """), {"id": run.sync_run_id}) == "STITCHING"
    # Separately cover a direct raw writer after the bulk loader has completed;
    # a preexisting extra member in the same batch correctly breaks exact replay.
    inserted = await session.execute(
        text("""
        INSERT INTO raw.customer_current(batch_id,source_system_id,source_customer_id,first_name)
        VALUES(:batch,:source,'direct-key','Alice') RETURNING raw_customer_id,app_id
        """),
        {"batch": run.ingestion_batch_id, "source": connection.source_system_id},
    )
    raw_id, app_id = inserted.one()
    assert app_id == "00042"
    await session.execute(
        text("""
        INSERT INTO staging.customer_profile
            (batch_id,raw_customer_id,source_system_id,source_customer_id)
        VALUES(:batch,:raw,:source,'direct-key')
        """),
        {"batch": run.ingestion_batch_id, "raw": raw_id, "source": connection.source_system_id},
    )
    assert await session.scalar(
        text("SELECT app_id FROM staging.customer_profile WHERE raw_customer_id=:id"),
        {"id": raw_id},
    ) == "00042"
    await session.commit()
