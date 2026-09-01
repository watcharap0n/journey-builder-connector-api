from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime
from typing import Any

import asyncpg  # type: ignore[import-untyped]
import boto3
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.modules.standardization.schemas import (
    SourcePartitionInventory,
    StandardizationRunView,
)


class StandardizationNotFoundError(LookupError):
    pass


class StandardizationConfigurationError(ValueError):
    pass


INVENTORY_SQL = """
SELECT indices.source_index,
       (
           SELECT max(document.source_id)
           FROM raw.elasticsearch_documents AS document
           WHERE document.source_index = indices.source_index
       ) AS baseline_cutoff_source_id,
       (
           SELECT max(version.ingest_id)
           FROM raw.elasticsearch_document_versions AS version
           WHERE version.source_index = indices.source_index
       ) AS version_cutoff_ingest_id
FROM migration.elasticsearch_indices AS indices
WHERE EXISTS (
          SELECT 1 FROM raw.elasticsearch_documents AS document
          WHERE document.source_index = indices.source_index
      )
   OR EXISTS (
          SELECT 1 FROM raw.elasticsearch_document_versions AS version
          WHERE version.source_index = indices.source_index
      )
ORDER BY indices.source_index
"""


async def discover_source_inventory(
    settings: Settings,
) -> list[SourcePartitionInventory]:
    if not settings.standardization_source_secret_id:
        raise StandardizationConfigurationError(
            "STANDARDIZATION_SOURCE_SECRET_ID is not configured"
        )
    client = boto3.client("secretsmanager", region_name=settings.aws_region)
    response = await asyncio.to_thread(
        client.get_secret_value,
        SecretId=settings.standardization_source_secret_id,
    )
    secret = json.loads(response["SecretString"])
    connection = await asyncpg.connect(
        host=secret["host"],
        port=int(secret.get("port", 5432)),
        user=secret.get("username", secret.get("user")),
        password=secret["password"],
        database=settings.standardization_source_database,
        ssl=settings.standardization_source_ssl,
        timeout=30,
        server_settings={"application_name": "standardization_run_planner"},
    )
    try:
        rows = await connection.fetch(INVENTORY_SQL)
    finally:
        await connection.close()
    return [SourcePartitionInventory.model_validate(dict(row)) for row in rows]


def _run_view(row: Any) -> StandardizationRunView:
    mapping = row._mapping if hasattr(row, "_mapping") else row
    return StandardizationRunView(
        run_id=mapping["run_id"],
        dataset_id=mapping["dataset_id"],
        workspace_id=mapping["workspace_id"],
        trigger_type=mapping["trigger_type"],
        run_status=mapping["run_status"],
        discovered_indexes=mapping["discovered_index_count"],
        queued_indexes=mapping["queued_index_count"],
        skipped_indexes=mapping["skipped_index_count"],
        source_records=mapping["source_record_count"],
        new_records=mapping["new_record_count"],
        duplicate_records=mapping["duplicate_record_count"],
        rejected_records=mapping["rejected_record_count"],
        quarantined_records=mapping["quarantined_record_count"],
        created_at=mapping["created_at"],
        started_at=mapping["started_at"],
        completed_at=mapping["completed_at"],
        error_code=mapping["error_code"],
        error_message=mapping["error_message"],
    )


RUN_SELECT = """
SELECT run_id, dataset_id, workspace_id, trigger_type, run_status,
       discovered_index_count, queued_index_count, skipped_index_count,
       source_record_count, new_record_count, duplicate_record_count,
       rejected_record_count, quarantined_record_count, created_at,
       started_at, completed_at, error_code, error_message
FROM control.standardization_run
"""


async def get_run(
    session: AsyncSession, workspace_id: uuid.UUID, run_id: uuid.UUID
) -> StandardizationRunView:
    row = (
        await session.execute(
            text(
                RUN_SELECT
                + " WHERE run_id = :run_id AND workspace_id = :workspace_id"
            ),
            {"run_id": run_id, "workspace_id": workspace_id},
        )
    ).first()
    if row is None:
        raise StandardizationNotFoundError("standardization run not found")
    return _run_view(row)


async def create_manual_run(
    session: AsyncSession,
    *,
    workspace_id: uuid.UUID,
    dataset_id: int,
    inventory: list[SourcePartitionInventory],
    idempotency_key: str | None,
) -> StandardizationRunView:
    await session.execute(
        text("SELECT pg_advisory_xact_lock(:dataset_id)"),
        {"dataset_id": dataset_id},
    )
    dataset = (
        await session.execute(
            text(
                """
                SELECT dataset_id, adapter_version, contract_version
                FROM control.standardization_dataset
                WHERE dataset_id = :dataset_id
                  AND (workspace_id = :workspace_id OR workspace_id IS NULL)
                  AND is_active
                """
            ),
            {"dataset_id": dataset_id, "workspace_id": workspace_id},
        )
    ).first()
    if dataset is None:
        raise StandardizationNotFoundError("standardization dataset not found")
    await session.execute(
        text(
            """
            UPDATE control.standardization_dataset
            SET workspace_id = :workspace_id, updated_at = now()
            WHERE dataset_id = :dataset_id AND workspace_id IS NULL
            """
        ),
        {"workspace_id": workspace_id, "dataset_id": dataset_id},
    )

    if idempotency_key:
        prior = (
            await session.execute(
                text(
                    RUN_SELECT
                    + """
                      WHERE dataset_id = :dataset_id
                        AND workspace_id = :workspace_id
                        AND source_run_key = :source_run_key
                      LIMIT 1
                    """
                ),
                {
                    "dataset_id": dataset_id,
                    "workspace_id": workspace_id,
                    "source_run_key": f"manual:{workspace_id}:{idempotency_key}",
                },
            )
        ).first()
        if prior is not None:
            await session.commit()
            return _run_view(prior)

    active = (
        await session.execute(
            text(
                RUN_SELECT
                + """
                  WHERE dataset_id = :dataset_id
                    AND run_status IN ('PENDING','RUNNING')
                  ORDER BY created_at DESC
                  LIMIT 1
                  FOR UPDATE
                """
            ),
            {"dataset_id": dataset_id},
        )
    ).first()
    if active is not None and active._mapping["workspace_id"] is not None:
        await session.commit()
        return _run_view(active)

    if active is None:
        run_id = uuid.uuid4()
        source_run_key = (
            f"manual:{workspace_id}:{idempotency_key}"
            if idempotency_key
            else f"manual:{workspace_id}:{run_id}"
        )
        await session.execute(
            text(
                """
                INSERT INTO control.standardization_run (
                    run_id, dataset_id, workspace_id, source_run_key,
                    run_status, trigger_type, adapter_version, contract_version
                ) VALUES (
                    :run_id, :dataset_id, :workspace_id, :source_run_key,
                    'PENDING', 'MANUAL', :adapter_version, :contract_version
                )
                """
            ),
            {
                "run_id": run_id,
                "dataset_id": dataset_id,
                "workspace_id": workspace_id,
                "source_run_key": source_run_key,
                "adapter_version": dataset._mapping["adapter_version"],
                "contract_version": dataset._mapping["contract_version"],
            },
        )
    else:
        run_id = active._mapping["run_id"]
        await session.execute(
            text(
                """
                UPDATE control.standardization_run
                SET workspace_id = :workspace_id, trigger_type = 'MANUAL',
                    updated_at = now()
                WHERE run_id = :run_id AND workspace_id IS NULL
                """
            ),
            {"workspace_id": workspace_id, "run_id": run_id},
        )

    queued_work_items = 0
    queued_index_names: set[str] = set()
    manifest: dict[str, dict[str, Any]] = {}
    for item in inventory:
        streams = (
            (
                "baseline",
                {"last_source_id": item.baseline_cutoff_source_id}
                if item.baseline_cutoff_source_id is not None
                else None,
            ),
            (
                "versions",
                {"last_ingest_id": item.version_cutoff_ingest_id}
                if item.version_cutoff_ingest_id is not None
                else None,
            ),
        )
        manifest[item.source_index] = {}
        for stream_kind, cutoff in streams:
            if cutoff is None:
                continue
            state = (
                await session.execute(
                    text(
                        """
                        SELECT committed_cursor, is_complete
                        FROM control.standardization_partition_state
                        WHERE dataset_id = :dataset_id
                          AND source_partition_key = :source_index
                          AND stream_kind = :stream_kind
                        """
                    ),
                    {
                        "dataset_id": dataset_id,
                        "source_index": item.source_index,
                        "stream_kind": stream_kind,
                    },
                )
            ).first()
            existing_checkpoint = (
                await session.execute(
                    text(
                        """
                        SELECT source_cursor
                        FROM control.standardization_checkpoint
                        WHERE run_id = :run_id
                          AND source_partition_key = :source_index
                          AND stream_kind = :stream_kind
                        """
                    ),
                    {
                        "run_id": run_id,
                        "source_index": item.source_index,
                        "stream_kind": stream_kind,
                    },
                )
            ).first()
            start = (
                dict(state._mapping["committed_cursor"])
                if state is not None
                else dict(existing_checkpoint._mapping["source_cursor"])
                if existing_checkpoint is not None
                else {}
            )
            start_value = start.get(
                "last_source_id" if stream_kind == "baseline" else "last_ingest_id"
            )
            cutoff_value = cutoff[
                "last_source_id" if stream_kind == "baseline" else "last_ingest_id"
            ]
            has_work = start_value is None or start_value < cutoff_value
            if state is None and existing_checkpoint is not None and start:
                await session.execute(
                    text(
                        """
                        INSERT INTO control.standardization_partition_state (
                            dataset_id, source_partition_key, stream_kind,
                            committed_cursor, committed_run_id, is_complete
                        ) VALUES (
                            :dataset_id, :source_index, :stream_kind,
                            CAST(:start AS jsonb), :run_id, :is_complete
                        )
                        ON CONFLICT (
                            dataset_id, source_partition_key, stream_kind
                        ) DO NOTHING
                        """
                    ),
                    {
                        "dataset_id": dataset_id,
                        "source_index": item.source_index,
                        "stream_kind": stream_kind,
                        "start": json.dumps(start),
                        "run_id": run_id,
                        "is_complete": stream_kind == "baseline" and not has_work,
                    },
                )
            manifest[item.source_index][stream_kind] = {
                "start": start,
                "cutoff": cutoff,
            }
            if not has_work:
                continue

            await session.execute(
                text(
                    """
                    INSERT INTO control.standardization_checkpoint (
                        run_id, dataset_id, source_partition_key, stream_kind,
                        checkpoint_status, source_cursor, start_cursor, cutoff_cursor
                    ) VALUES (
                        :run_id, :dataset_id, :source_index, :stream_kind,
                        'PENDING', CAST(:start AS jsonb), CAST(:start AS jsonb),
                        CAST(:cutoff AS jsonb)
                    )
                    ON CONFLICT (run_id, source_partition_key, stream_kind)
                    DO UPDATE SET cutoff_cursor = EXCLUDED.cutoff_cursor,
                                  updated_at = now()
                    """
                ),
                {
                    "run_id": run_id,
                    "dataset_id": dataset_id,
                    "source_index": item.source_index,
                    "stream_kind": stream_kind,
                    "start": json.dumps(start),
                    "cutoff": json.dumps(cutoff),
                },
            )
            payload = {
                "message_version": 1,
                "operation_type": "ELASTICSEARCH_STANDARDIZE",
                "run_id": str(run_id),
                "dataset_id": dataset_id,
                "source_index": item.source_index,
                "stream_kind": stream_kind,
            }
            await session.execute(
                text(
                    """
                    INSERT INTO control.standardization_outbox_event (
                        workspace_id, run_id, source_partition_key, stream_kind,
                        event_type, deduplication_key, payload_json
                    ) VALUES (
                        :workspace_id, :run_id, :source_index, :stream_kind,
                        'standardization.partition.dispatch', :deduplication_key,
                        CAST(:payload AS jsonb)
                    )
                    ON CONFLICT (deduplication_key) DO NOTHING
                    """
                ),
                {
                    "workspace_id": workspace_id,
                    "run_id": run_id,
                    "source_index": item.source_index,
                    "stream_kind": stream_kind,
                    "deduplication_key": f"standardize:{run_id}:{item.source_index}:{stream_kind}",
                    "payload": json.dumps(payload),
                },
            )
            queued_work_items += 1
            queued_index_names.add(item.source_index)

    queued = len(queued_index_names)
    skipped = len(inventory) - queued
    status = "PENDING" if queued_work_items else "SUCCEEDED"
    completed_at = None if queued_work_items else datetime.now(UTC)
    await session.execute(
        text(
            """
            UPDATE control.standardization_run
            SET run_status = :status,
                cutoff_manifest = CAST(:manifest AS jsonb),
                discovered_index_count = :discovered,
                queued_index_count = :queued,
                skipped_index_count = :skipped,
                completed_at = :completed_at,
                updated_at = now()
            WHERE run_id = :run_id
            """
        ),
        {
            "status": status,
            "manifest": json.dumps(manifest),
            "discovered": len(inventory),
            "queued": queued,
            "skipped": skipped,
            "completed_at": completed_at,
            "run_id": run_id,
        },
    )
    await session.commit()
    return await get_run(session, workspace_id, run_id)
