from typing import Any

import pytest

from app.core.config import Settings
from app.main import create_app
from app.modules.standardization.schemas import SourcePartitionInventory
from app.modules.standardization.service import INVENTORY_SQL, discover_source_inventory


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
        async def fetch(self, _query: str) -> list[object]:
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

    inventory = await discover_source_inventory(configured)

    assert inventory == []
    assert captured["ssl"] == "require"
