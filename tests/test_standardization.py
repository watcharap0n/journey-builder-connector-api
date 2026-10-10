import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from app.core.config import Settings
from app.main import create_app
from app.modules.standardization import controller
from app.modules.standardization.schemas import SourcePartitionInventory
from app.modules.standardization.service import (
    INVENTORY_SQL,
    StandardizationConfigurationError,
    StandardizationNotFoundError,
    create_manual_run,
    discover_source_inventory,
    get_run,
)

WORKSPACE_ID = uuid.UUID("00000000-0000-4000-8000-000000000002")
RUN_ID = uuid.UUID("00000000-0000-4000-8000-000000000003")


class _Row:
    def __init__(self, values: dict[str, Any]) -> None:
        self._mapping = values


class _Result:
    def __init__(self, values: dict[str, Any] | None = None) -> None:
        self.row = _Row(values) if values is not None else None

    def first(self) -> _Row | None:
        return self.row


class _RunSession:
    def __init__(self, active: dict[str, Any] | None = None) -> None:
        self.active = active
        self.inserted: dict[str, Any] | None = None
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.commits = 0

    async def execute(self, statement: object, params: dict[str, Any]) -> _Result:
        sql = str(statement)
        self.calls.append((sql, params))
        if "FROM control.standardization_dataset" in sql:
            return _Result({"adapter_version": "v1", "contract_version": "v1"})
        if "FROM control.standardization_run" in sql:
            if "FOR UPDATE" in sql:
                if self.active and (
                    "run_scope = 'legacy'" not in sql
                    or self.active["run_scope"] == "legacy"
                ):
                    return _Result(self.active)
                return _Result()
            if "source_run_key" in sql:
                return _Result()
            if "run_scope = 'legacy'" in sql:
                if self.inserted and params["run_id"] == self.inserted["run_id"]:
                    return _Result(self.inserted)
                return _Result(
                    self.active if self.active and self.active["run_scope"] == "legacy" else None
                )
            if self.inserted and params["run_id"] == self.inserted["run_id"]:
                return _Result(self.inserted)
            return _Result(self.active)
        if "INSERT INTO control.standardization_run" in sql:
            self.inserted = _run_row(scope="legacy", workspace_id=params["workspace_id"])
            self.inserted["run_id"] = params["run_id"]
        return _Result()

    async def commit(self) -> None:
        self.commits += 1


def _run_row(*, scope: str, workspace_id: uuid.UUID | None) -> dict[str, Any]:
    return {
        "run_id": RUN_ID,
        "dataset_id": 1,
        "workspace_id": workspace_id,
        "run_scope": scope,
        "trigger_type": "MANUAL",
        "run_status": "RUNNING",
        "discovered_index_count": 0,
        "queued_index_count": 0,
        "skipped_index_count": 0,
        "source_record_count": 0,
        "new_record_count": 0,
        "duplicate_record_count": 0,
        "rejected_record_count": 0,
        "quarantined_record_count": 0,
        "created_at": datetime.now(UTC),
        "started_at": None,
        "completed_at": None,
        "error_code": None,
        "error_message": None,
    }


async def test_get_run_cannot_read_baseline_2026_even_with_matching_workspace() -> None:
    session = _RunSession(_run_row(scope="baseline-2026", workspace_id=WORKSPACE_ID))

    with pytest.raises(StandardizationNotFoundError):
        await get_run(session, WORKSPACE_ID, RUN_ID, legacy_scope_enabled=True)  # type: ignore[arg-type]


async def test_manual_run_ignores_2026_active_and_inserts_explicit_legacy() -> None:
    session = _RunSession(_run_row(scope="baseline-2026", workspace_id=None))

    await create_manual_run(
        session,  # type: ignore[arg-type]
        workspace_id=WORKSPACE_ID,
        dataset_id=1,
        inventory=[],
        idempotency_key="key",
        legacy_scope_enabled=True,
    )

    selects = [sql for sql, _ in session.calls if "FROM control.standardization_run" in sql]
    assert len(selects) == 3  # idempotency, active, final read
    assert all("run_scope = 'legacy'" in sql for sql in selects)
    insert = next(
        sql for sql, _ in session.calls if "INSERT INTO control.standardization_run" in sql
    )
    assert "run_scope" in insert and "'legacy'" in insert
    assert not any(
        "SET workspace_id = :workspace_id, trigger_type" in sql for sql, _ in session.calls
    )


async def test_manual_run_reuses_existing_2025_active_legacy_run() -> None:
    session = _RunSession(_run_row(scope="legacy", workspace_id=WORKSPACE_ID))

    run = await create_manual_run(
        session,  # type: ignore[arg-type]
        workspace_id=WORKSPACE_ID,
        dataset_id=1,
        inventory=[],
        idempotency_key=None,
        legacy_scope_enabled=True,
    )

    assert run.run_id == RUN_ID
    assert not any("INSERT INTO control.standardization_run" in sql for sql, _ in session.calls)
    assert session.commits == 1


async def test_get_run_reads_2025_legacy_run_with_scope_enabled() -> None:
    session = _RunSession(_run_row(scope="legacy", workspace_id=WORKSPACE_ID))

    run = await get_run(
        session, WORKSPACE_ID, RUN_ID, legacy_scope_enabled=True  # type: ignore[arg-type]
    )

    assert run.run_id == RUN_ID
    assert "run_scope = 'legacy'" in session.calls[0][0]


async def test_idempotency_replay_is_legacy_scoped() -> None:
    session = _RunSession(_run_row(scope="legacy", workspace_id=WORKSPACE_ID))

    await create_manual_run(
        session,  # type: ignore[arg-type]
        workspace_id=WORKSPACE_ID,
        dataset_id=1,
        inventory=[],
        idempotency_key="repeat",
        legacy_scope_enabled=True,
    )

    replay_sql = next(sql for sql, _ in session.calls if "source_run_key = :source_run_key" in sql)
    assert "run_scope = 'legacy'" in replay_sql


@pytest.mark.parametrize("legacy_scope_enabled", [False, True])
async def test_manual_run_never_adopts_null_workspace_legacy_run(
    legacy_scope_enabled: bool,
) -> None:
    session = _RunSession(_run_row(scope="legacy", workspace_id=None))

    with pytest.raises(StandardizationConfigurationError, match="unowned legacy run"):
        await create_manual_run(
            session,  # type: ignore[arg-type]
            workspace_id=WORKSPACE_ID,
            dataset_id=1,
            inventory=[],
            idempotency_key=None,
            legacy_scope_enabled=legacy_scope_enabled,
        )

    assert not any("UPDATE control.standardization_run" in sql for sql, _ in session.calls)
    assert not any("INSERT INTO control.standardization_run" in sql for sql, _ in session.calls)


async def test_scope_guard_is_off_for_pre_migration_schema() -> None:
    session = _RunSession(_run_row(scope="legacy", workspace_id=WORKSPACE_ID))
    await create_manual_run(
        session,  # type: ignore[arg-type]
        workspace_id=WORKSPACE_ID,
        dataset_id=1,
        inventory=[],
        idempotency_key=None,
        legacy_scope_enabled=False,
    )

    assert not any("run_scope" in sql for sql, _ in session.calls)


async def test_pre_migration_new_run_inserts_without_scope_column() -> None:
    session = _RunSession()

    run = await create_manual_run(
        session,  # type: ignore[arg-type]
        workspace_id=WORKSPACE_ID,
        dataset_id=1,
        inventory=[],
        idempotency_key=None,
        legacy_scope_enabled=False,
    )

    insert = next(
        sql for sql, _ in session.calls if "INSERT INTO control.standardization_run" in sql
    )
    assert "run_scope" not in insert
    assert run.workspace_id == WORKSPACE_ID


def test_legacy_scope_is_opt_in_until_schema_migration(settings: Settings) -> None:
    assert settings.standardization_legacy_scope_enabled is False


async def test_post_run_forwards_dataset_and_scope_gate(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    observed: dict[str, Any] = {}

    async def inventory(_settings: Settings, dataset_id: int) -> list[SourcePartitionInventory]:
        observed["inventory_dataset_id"] = dataset_id
        return []

    async def create(_session: object, **kwargs: Any) -> object:
        observed.update(kwargs)
        return "created"

    monkeypatch.setattr(controller, "discover_source_inventory", inventory)
    monkeypatch.setattr(controller, "create_manual_run", create)
    enabled = settings.model_copy(update={"standardization_legacy_scope_enabled": True})

    result = await controller.post_standardization_run(
        WORKSPACE_ID, 1, object(), enabled, None  # type: ignore[arg-type]
    )

    assert str(result) == "created"
    assert observed["inventory_dataset_id"] == 1
    assert observed["legacy_scope_enabled"] is True


def test_openapi_exposes_manual_standardization_routes(settings: Settings) -> None:
    paths = create_app(settings).openapi()["paths"]
    assert (
        "/api/v1/workspaces/{workspace_id}/standardization-datasets/{dataset_id}/runs"
        in paths
    )
    assert "/api/v1/workspaces/{workspace_id}/standardization-runs/{run_id}" in paths
    post = paths[
        "/api/v1/workspaces/{workspace_id}/standardization-datasets/{dataset_id}/runs"
    ]["post"]
    assert "requestBody" not in post


def test_inventory_tracks_both_baseline_and_incremental_cutoffs() -> None:
    inventory = SourcePartitionInventory(
        source_index="trackers_202409",
        baseline_cutoff_source_id="z-last",
        version_cutoff_ingest_id=42,
    )
    assert inventory.version_cutoff_ingest_id == 42
    assert "indices.status" not in INVENTORY_SQL
    assert "max(version.ingest_id)" in INVENTORY_SQL
    assert "FROM migration.elasticsearch_indices" in INVENTORY_SQL
    assert "EXISTS" in INVENTORY_SQL


@pytest.mark.parametrize("dataset_id", [1, 7])
def test_inventory_predicate_preserves_versions_only_and_exact_index_prefix(
    dataset_id: int,
) -> None:
    db = sqlite3.connect(":memory:")
    try:
        db.executescript("""
            CREATE TABLE indices(source_index text);
            CREATE TABLE documents(source_index text, source_id text);
            CREATE TABLE versions(source_index text, ingest_id int);
        """)
        names = ("trackers_202501", "trackers_202601", "trackersX202601", "sales_202601")
        db.executemany("INSERT INTO indices VALUES (?)", [(name,) for name in names])
        db.executemany("INSERT INTO documents VALUES (?, ?)", [
            ("trackers_202501", "a"), ("trackers_202601", "b"),
            ("trackersX202601", "c"),
        ])
        db.execute("INSERT INTO versions VALUES ('sales_202601', 1)")
        query = (INVENTORY_SQL
                 .replace("migration.elasticsearch_indices", "indices")
                 .replace("raw.elasticsearch_documents", "documents")
                 .replace("raw.elasticsearch_document_versions", "versions")
                 .replace("$1", "?"))

        rows = db.execute(query, (dataset_id,)).fetchall()
    finally:
        db.close()

    assert {row[0] for row in rows} == (
        {"trackers_202501", "trackersX202601", "sales_202601"}
        if dataset_id == 1
        else set(names)
    )


async def test_inventory_connection_uses_configured_sslmode(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    captured: dict[str, object] = {}

    class _SecretsManagerClient:
        def get_secret_value(self, *, SecretId: str) -> dict[str, str]:
            assert SecretId == "source-secret"
            return {
                "SecretString": (
                    '{"host":"database.internal","port":5432,'
                    '"username":"reader","password":"secret"}'
                )
            }

    class _Connection:
        async def fetch(self, _query: str, _dataset_id: int) -> list[object]:
            return []

        async def close(self) -> None:
            return None

    async def _connect(**kwargs: Any) -> _Connection:
        captured.update(kwargs)
        return _Connection()

    monkeypatch.setattr(
        "app.modules.standardization.service.boto3.client",
        lambda *_args, **_kwargs: _SecretsManagerClient(),
    )
    monkeypatch.setattr("app.modules.standardization.service.asyncpg.connect", _connect)
    configured = settings.model_copy(
        update={
            "standardization_source_secret_id": "source-secret",
            "standardization_source_sslmode": "require",
        }
    )

    inventory = await discover_source_inventory(configured, dataset_id=7)

    assert inventory == []
    assert captured["ssl"] == "require"


@pytest.mark.parametrize("dataset_id", [1, 7])
async def test_inventory_excludes_trackers_2026_only_for_dataset_1(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, dataset_id: int
) -> None:
    observed: list[tuple[str, tuple[object, ...]]] = []

    class _Secrets:
        def get_secret_value(self, **_kwargs: Any) -> dict[str, str]:
            return {"SecretString": '{"host":"db","username":"u","password":"p"}'}

    class _Connection:
        async def fetch(self, query: str, *args: object) -> list[dict[str, Any]]:
            observed.append((query, args))
            # Simulate source SQL's family predicate on representative data.
            rows = [
                {"source_index": "trackers_202501", "baseline_cutoff_source_id": "a"},
                {"source_index": "trackers_202601", "baseline_cutoff_source_id": "b"},
                {"source_index": "sales_202601", "baseline_cutoff_source_id": "c"},
            ]
            if "NOT LIKE 'trackers!_2026%' ESCAPE '!'" in query and args == (1,):
                return [row for row in rows if not row["source_index"].startswith("trackers_2026")]
            return rows

        async def close(self) -> None:
            return None

    async def connect(**_kwargs: Any) -> _Connection:
        return _Connection()

    monkeypatch.setattr(
        "app.modules.standardization.service.boto3.client", lambda *_a, **_k: _Secrets()
    )
    monkeypatch.setattr("app.modules.standardization.service.asyncpg.connect", connect)
    configured = settings.model_copy(update={"standardization_source_secret_id": "source"})

    inventory = await discover_source_inventory(configured, dataset_id=dataset_id)

    assert observed[0][1] == (dataset_id,)
    assert "NOT LIKE 'trackers!_2026%' ESCAPE '!'" in observed[0][0]
    assert "AND (" in observed[0][0]  # parenthesized original baseline/versions OR
    assert {item.source_index for item in inventory} == (
        {"trackers_202501", "sales_202601"}
        if dataset_id == 1
        else {"trackers_202501", "trackers_202601", "sales_202601"}
    )
