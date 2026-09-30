"""Opt-in API + Alembic verification on disposable local PostgreSQL (port 55441)."""

import importlib.util
import os
import uuid
from collections.abc import AsyncIterator
from pathlib import Path

import asyncpg
import pytest
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from app.infrastructure.database.base import Base
from app.infrastructure.database.session import get_db_session
from app.main import create_app

pytestmark = pytest.mark.skipif(
    os.getenv("CDP_CONNECTOR_APP_PG_TESTS") != "1", reason="opt-in local PostgreSQL integration"
)
MIGRATION_PATH = Path(__file__).resolve().parents[1] / "alembic/versions/005_workspace_app_id.py"


@pytest.fixture
async def db() -> AsyncIterator[tuple[object, AsyncSession]]:
    admin = await asyncpg.connect(
        host="127.0.0.1", port=55441, user="postgres", database="postgres"
    )
    name = "cdp_workspace_app_" + uuid.uuid4().hex
    assert not await admin.fetchval("SELECT 1 FROM pg_database WHERE datname=$1", name)
    await admin.execute(f'CREATE DATABASE "{name}" TEMPLATE template0 ENCODING \'UTF8\'')
    engine = create_async_engine(f"postgresql+asyncpg://postgres@127.0.0.1:55441/{name}")
    try:
        async with engine.begin() as connection:
            await connection.execute(text("CREATE SCHEMA platform"))
            await connection.execute(text("""
                CREATE TABLE platform.workspace (
                    workspace_id uuid PRIMARY KEY,
                    slug varchar(63) NOT NULL CONSTRAINT workspace_slug_key UNIQUE,
                    name varchar(255) NOT NULL, is_active boolean NOT NULL DEFAULT true,
                    created_at timestamptz NOT NULL DEFAULT now(),
                    updated_at timestamptz NOT NULL DEFAULT now()
                )
            """))
            spec = importlib.util.spec_from_file_location("workspace_app_migration", MIGRATION_PATH)
            assert spec and spec.loader
            revision = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(revision)

            def migrate(sync_connection: Connection) -> None:
                context = MigrationContext.configure(
                    sync_connection, opts={"target_metadata": Base.metadata}
                )
                with Operations.context(context):
                    revision.upgrade()

            await connection.run_sync(migrate)
        async with AsyncSession(engine, expire_on_commit=False) as session:
            yield engine, session
    finally:
        await engine.dispose()
        assert await admin.fetchval("SELECT 1 FROM pg_database WHERE datname=$1", name)
        await admin.execute(f'DROP DATABASE "{name}"')
        assert not await admin.fetchval("SELECT 1 FROM pg_database WHERE datname=$1", name)
        await admin.close()


async def test_workspace_migration_and_http_round_trip(db: tuple[object, AsyncSession]) -> None:
    _, session = db
    app = create_app()
    app.dependency_overrides[get_db_session] = lambda: session
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        created = await client.post("/api/v1/workspaces", json={
            "slug": "app-42", "name": "App 42", "app_id": "00042"
        })
        assert created.status_code == 201
        key = created.json()["workspace_id"]
        assert created.json()["app_id"] == "00042"
        fetched = await client.get(f"/api/v1/workspaces/{key}")
        assert fetched.status_code == 200
        assert fetched.json()["app_id"] == "00042"
    stored = await session.scalar(
        text("SELECT app_id FROM platform.workspace WHERE workspace_id=:key"),
        {"key": uuid.UUID(key)},
    )
    assert stored == "00042"
    names = (await session.execute(text("""
        SELECT conname FROM pg_constraint WHERE conrelid='platform.workspace'::regclass
          AND contype='c'
    """))).scalars().all()
    assert names == ["ck_workspace_workspace_app_id_decimal"]


async def test_duplicate_workspace_slug_returns_conflict_and_preserves_existing_row(
    db: tuple[object, AsyncSession],
) -> None:
    _, session = db
    app = create_app()
    app.dependency_overrides[get_db_session] = lambda: session
    payload = {
        "slug": "postgres-connector-poc-13",
        "name": "PostgreSQL Connector POC",
        "app_id": "456",
    }
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        first = await client.post("/api/v1/workspaces", json=payload)
        assert first.status_code == 201, first.text
        repeated = await client.post("/api/v1/workspaces", json=payload)
        assert repeated.status_code == 409, repeated.text
        assert repeated.json() == {"detail": "workspace slug already exists"}
        existing = await client.get(f"/api/v1/workspaces/{first.json()['workspace_id']}")
        assert existing.status_code == 200
        assert existing.json()["app_id"] == "456"
        another = await client.post(
            "/api/v1/workspaces", json={**payload, "slug": "another-postgres-poc"}
        )
        assert another.status_code == 201, another.text


async def test_workspace_migration_refuses_downgrade_while_connector_lineage_installed(
    db: tuple[AsyncEngine, AsyncSession],
) -> None:
    engine, session = db
    await session.rollback()
    async with engine.begin() as connection:
        await connection.execute(text("CREATE SCHEMA control"))
        await connection.execute(text("""
            CREATE FUNCTION control.connector_batch_app_scope(uuid)
            RETURNS TABLE(app_id text,error_code text) LANGUAGE sql AS $$
                SELECT NULL::text,NULL::text
            $$
        """))
        spec = importlib.util.spec_from_file_location("workspace_app_downgrade", MIGRATION_PATH)
        assert spec and spec.loader
        revision = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(revision)

        def downgrade(sync_connection: Connection) -> None:
            context = MigrationContext.configure(sync_connection)
            with Operations.context(context):
                revision.downgrade()

        with pytest.raises(RuntimeError, match="connector app lineage"):
            await connection.run_sync(downgrade)
        remains = await connection.scalar(text("""
            SELECT count(*) FROM pg_attribute
            WHERE attrelid='platform.workspace'::regclass AND attname='app_id' AND NOT attisdropped
        """))
        assert remains == 1
