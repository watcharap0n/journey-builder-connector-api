import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import ValidationError

from app.core.config import Settings
from app.modules.connectors import service
from app.modules.connectors.entities import TARGET_ENTITIES
from app.modules.connectors.model import ConnectorDataset, SchemaSnapshot
from app.modules.connectors.schemas import (
    ConnectionCreate,
    DatasetUpdate,
    MappingCreate,
    UploadDatasetCreate,
)

SCHEMA = {"columns": [{"name": n, "type": "text"} for n in ("doc", "id", "mobile", "vin", "at")]}
MAPPING_V2 = {
    "schema_snapshot_id": str(uuid.uuid4()),
    "version": 2,
    "derived_keys": {
        "buyer": {"first_of": [{"id_type": "nid", "source_path": "id",
                                "transforms": ["alnum", "hmac"]},
                               {"id_type": "phone", "source_path": "mobile",
                                "transforms": ["th_mobile"]}]},
        "vehicle": {"first_of": [{"id_type": "vin", "source_path": "vin",
                                  "transforms": ["alnum"]}]},
    },
    "projections": [
        {"name": "buyer", "target_entity": "CUSTOMER", "when_present": ["buyer"],
         "fields": [{"target_field": "source_customer_id", "derived_key": "buyer"}]},
        {"name": "sale", "target_entity": "TRANSACTION", "fields": [
            {"target_field": "source_transaction_id", "source_path": "doc"},
            {"target_field": "source_customer_id", "derived_key": "buyer"},
            {"target_field": "transaction_type", "constant": "vehicle_sale"},
            {"target_field": "occurred_at", "source_path": "at",
             "transforms": ["parse_timestamp"]},
            {"target_field": "amount", "constant": 1},
            {"target_field": "currency", "constant": "THB"}]},
        {"name": "owns", "target_entity": "RELATIONSHIP", "when_present": ["buyer", "vehicle"],
         "fields": [{"target_field": "from_type", "constant": "customer"},
                    {"target_field": "from_id", "derived_key": "buyer"},
                    {"target_field": "relation_type", "constant": "buyer"},
                    {"target_field": "to_type", "constant": "asset"},
                    {"target_field": "to_id", "derived_key": "vehicle"}]},
    ],
}


def test_target_entities_include_graph_types_and_multi() -> None:
    assert TARGET_ENTITIES == ("CUSTOMER", "TRANSACTION", "EVENT", "ASSET", "RELATIONSHIP",
                               "MULTI")
    for entity in ("MULTI", "ASSET", "RELATIONSHIP"):
        assert DatasetUpdate(target_entity=entity).target_entity == entity


def test_file_upload_connections_take_no_credentials() -> None:
    connection = ConnectionCreate(engine="file_upload", name="Dealer CRM", source_code="dealer")
    assert connection.credentials is None
    with pytest.raises(ValidationError, match="take no credentials"):
        ConnectionCreate(engine="file_upload", name="x", source_code="x",
                         credentials={"host": "h", "username": "u", "password": "p"})
    with pytest.raises(ValidationError, match="credentials are required"):
        ConnectionCreate(engine="postgresql", name="x", source_code="x")


def test_mapping_create_shapes() -> None:
    v2 = MappingCreate(**MAPPING_V2)
    assert v2.version == 2 and v2.fields is None
    with pytest.raises(ValidationError, match="needs projections"):
        MappingCreate(schema_snapshot_id=uuid.uuid4(), version=2,
                      fields=[{"source_path": "id", "target_field": "source_customer_id"}])
    with pytest.raises(ValidationError, match="use version 2"):
        MappingCreate(schema_snapshot_id=uuid.uuid4(), projections=MAPPING_V2["projections"])


def test_mapping_v2_is_checked_against_the_contract_and_the_header() -> None:
    payload = MappingCreate(**MAPPING_V2)
    service._validate_mapping_v2(SCHEMA, service._mapping_v2_json(payload))

    with pytest.raises(service.ConnectorConfigurationError, match="not present.*mobile"):
        service._validate_mapping_v2({"columns": [{"name": "doc"}, {"name": "id"},
                                                  {"name": "vin"}, {"name": "at"}]},
                                     service._mapping_v2_json(payload))
    broken = dict(MAPPING_V2, projections=[{"name": "x", "target_entity": "EVENT", "fields": [
        {"target_field": "source_event_id", "constant": "fixed"}]}])
    with pytest.raises(service.ConnectorConfigurationError) as exc:
        service._validate_mapping_v2(SCHEMA, service._mapping_v2_json(MappingCreate(**broken)))
    assert "the key cannot be a constant" in str(exc.value)
    assert "event requires event_name" in str(exc.value)


class _Repo:
    def __init__(self, connection: Any) -> None:
        self.connection = connection

    async def get_connection(self, *_args: Any) -> Any:
        return self.connection


@pytest.mark.asyncio
async def test_worker_operations_refuse_file_upload_connections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = SimpleNamespace(status="READY", definition_key="file_upload")
    monkeypatch.setattr(service, "ConnectorRepository", lambda _session: _Repo(connection))
    with pytest.raises(service.ConnectorConflictError, match="fed by uploads"):
        await service.queue_operation(object(), workspace_id=uuid.uuid4(),  # type: ignore[arg-type]
                                      connection_id=uuid.uuid4(), operation_type="DATASET_SYNC")


class _Session:
    def __init__(self, scalar_results: list[Any] | None = None) -> None:
        self.added: list[Any] = []
        self.scalar_results = list(scalar_results or [])
        self.committed = 0

    def add(self, obj: Any) -> None:
        self.added.append(obj)

    async def flush(self) -> None:
        for obj in self.added:
            for attr in ("dataset_id", "schema_snapshot_id", "connection_id"):
                if hasattr(obj, attr) and getattr(obj, attr) is None:
                    setattr(obj, attr, uuid.uuid4())

    async def commit(self) -> None:
        await self.flush()
        self.committed += 1

    async def refresh(self, _obj: Any) -> None:
        return None

    async def scalar(self, _stmt: Any) -> Any:
        return self.scalar_results.pop(0) if self.scalar_results else None

    async def scalars(self, _stmt: Any) -> Any:
        return SimpleNamespace(all=lambda: [])


@pytest.mark.asyncio
async def test_file_upload_connection_is_ready_without_a_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def no_workspace_check(*_args: Any) -> None:
        return None

    async def no_secret(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("file uploads must not create a secret")

    monkeypatch.setattr(service, "get_workspace", no_workspace_check)
    monkeypatch.setattr(service, "_put_secret", no_secret)
    session = _Session()
    connection = await service.create_connection(
        session,  # type: ignore[arg-type]
        Settings(_env_file=None), uuid.uuid4(),
        ConnectionCreate(engine="file_upload", name="Dealer CRM", source_code="dealer"),
    )
    assert connection.status == "READY" and connection.secret_arn is None
    assert session.committed == 1


@pytest.mark.asyncio
async def test_upload_dataset_records_the_normalised_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = SimpleNamespace(definition_key="file_upload", source_code="dealer")
    monkeypatch.setattr(service, "ConnectorRepository", lambda _session: _Repo(connection))
    session = _Session(scalar_results=[None, None])
    dataset, snapshot, columns, source_code = await service.create_upload_dataset(
        session,  # type: ignore[arg-type]
        uuid.uuid4(), uuid.uuid4(),
        UploadDatasetCreate(object_name="ccer", columns=["เลขที่\nใบจอง", "ราคาขาย", "ราคาขาย"]),
    )
    assert isinstance(dataset, ConnectorDataset) and isinstance(snapshot, SchemaSnapshot)
    assert dataset.target_entity == "MULTI" and dataset.status == "DISCOVERED"
    assert columns == ["เลขที่ ใบจอง", "ราคาขาย", "ราคาขาย #2"]
    assert [c["name"] for c in snapshot.schema_json["columns"]] == columns
    assert source_code == "dealer"

    db_connection = SimpleNamespace(definition_key="postgresql", source_code="db")
    monkeypatch.setattr(service, "ConnectorRepository", lambda _session: _Repo(db_connection))
    with pytest.raises(service.ConnectorConflictError, match="file_upload connection"):
        await service.create_upload_dataset(
            _Session(),  # type: ignore[arg-type]
            uuid.uuid4(), uuid.uuid4(), UploadDatasetCreate(object_name="x", columns=["a"]),
        )


def test_migration_008_extends_types_and_registers_file_upload() -> None:
    path = Path(__file__).parents[1] / "alembic/versions/008_intake_v2_multi_and_file_upload.py"
    text = path.read_text()
    assert "'CUSTOMER', 'TRANSACTION', 'EVENT', 'ASSET', 'RELATIONSHIP', 'MULTI'" in text
    assert "'file_upload', 'file', 'builtin.upload.v1'" in text
    assert 'down_revision = "007_generic_dataset_entities"' in text


def test_intake_outputs_route_is_exposed_and_off_by_default() -> None:
    from app.main import create_app

    paths = set(create_app(Settings(_env_file=None)).openapi()["paths"])
    assert any(p.endswith("/datasets/{dataset_id}/intake-outputs") for p in paths)
    assert any(p.endswith("/connectors/{connection_id}/upload-datasets") for p in paths)
    assert Settings(_env_file=None).connector_intake_status_enabled is False
