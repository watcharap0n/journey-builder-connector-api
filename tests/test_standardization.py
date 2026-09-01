from app.main import create_app
from app.modules.standardization.schemas import SourcePartitionInventory
from app.modules.standardization.service import INVENTORY_SQL


def test_openapi_exposes_manual_standardization_routes(settings) -> None:
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
