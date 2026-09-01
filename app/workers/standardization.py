from __future__ import annotations

import argparse
import asyncio
import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import Settings, get_settings
from app.infrastructure.secrets.aws import resolve_database_url

LOGGER = logging.getLogger(__name__)


async def _publish_dispatch(settings: Settings, payload: dict[str, Any]) -> None:
    if not settings.standardization_dispatch_queue_url:
        raise RuntimeError("STANDARDIZATION_DISPATCH_QUEUE_URL is not configured")
    client = boto3.client("sqs", region_name=settings.aws_region)
    await asyncio.to_thread(
        client.send_message,
        QueueUrl=settings.standardization_dispatch_queue_url,
        MessageBody=json.dumps(payload, separators=(",", ":")),
    )


async def process_outbox_once(
    session_factory: async_sessionmaker[AsyncSession], settings: Settings
) -> bool:
    async with session_factory() as session:
        async with session.begin():
            event = (
                await session.execute(
                    text(
                        """
                        SELECT outbox_event_id, payload_json, attempt
                        FROM control.standardization_outbox_event
                        WHERE status IN ('PENDING','FAILED','PROCESSING')
                          AND available_at <= now()
                        ORDER BY created_at
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                        """
                    )
                )
            ).first()
            if event is None:
                return False
            event_id = event._mapping["outbox_event_id"]
            attempt = int(event._mapping["attempt"]) + 1
            payload = dict(event._mapping["payload_json"])
            await session.execute(
                text(
                    """
                    UPDATE control.standardization_outbox_event
                    SET status = 'PROCESSING', attempt = :attempt,
                        available_at = now() + interval '5 minutes'
                    WHERE outbox_event_id = :event_id
                    """
                ),
                {"attempt": attempt, "event_id": event_id},
            )
        try:
            await _publish_dispatch(settings, payload)
            await session.execute(
                text(
                    """
                    UPDATE control.standardization_outbox_event
                    SET status = 'PROCESSED', processed_at = now(), last_error = NULL
                    WHERE outbox_event_id = :event_id
                    """
                ),
                {"event_id": event_id},
            )
            await session.execute(
                text(
                    """
                    UPDATE control.standardization_run
                    SET run_status = 'RUNNING', started_at = COALESCE(started_at, now()),
                        updated_at = now()
                    WHERE run_id = :run_id AND run_status = 'PENDING'
                    """
                ),
                {"run_id": uuid.UUID(payload["run_id"])},
            )
            await session.commit()
        except Exception as exc:
            await session.execute(
                text(
                    """
                    UPDATE control.standardization_outbox_event
                    SET status = 'FAILED', last_error = :error,
                        available_at = :available_at
                    WHERE outbox_event_id = :event_id
                    """
                ),
                {
                    "error": type(exc).__name__,
                    "available_at": datetime.now(UTC)
                    + timedelta(seconds=min(300, 2 ** min(attempt, 8))),
                    "event_id": event_id,
                },
            )
            await session.commit()
            LOGGER.exception("standardization outbox event failed id=%s", event_id)
        return True


async def process_result_message(
    session_factory: async_sessionmaker[AsyncSession], payload: dict[str, Any]
) -> None:
    run_id = uuid.UUID(payload["run_id"])
    source_index = str(payload["source_index"])
    stream_kind = str(payload["stream_kind"])
    terminal_status = str(payload["status"])
    if terminal_status not in {"SUCCEEDED", "FAILED"}:
        raise ValueError("standardization result status must be terminal")
    metrics = payload.get("metrics") or {}
    async with session_factory() as session:
        if terminal_status == "FAILED":
            checkpoint = (
                await session.execute(
                    text(
                        """
                        SELECT checkpoint.attempt, checkpoint.checkpoint_status,
                               checkpoint.dataset_id, run.workspace_id
                        FROM control.standardization_checkpoint AS checkpoint
                        JOIN control.standardization_run AS run USING (run_id)
                        WHERE checkpoint.run_id = :run_id
                          AND checkpoint.source_partition_key = :source_index
                          AND checkpoint.stream_kind = :stream_kind
                        FOR UPDATE OF checkpoint
                        """
                    ),
                    {
                        "run_id": run_id,
                        "source_index": source_index,
                        "stream_kind": stream_kind,
                    },
                )
            ).first()
            if checkpoint is None:
                raise RuntimeError("standardization checkpoint not found")
            checkpoint_values = checkpoint._mapping
            if checkpoint_values["checkpoint_status"] == "FAILED":
                attempt = int(checkpoint_values["attempt"])
                retrying = attempt < 3
                await session.execute(
                    text(
                        """
                        UPDATE control.standardization_checkpoint
                        SET checkpoint_status = :checkpoint_status,
                            error_code = :error_code,
                            error_message = :error_message, updated_at = now()
                        WHERE run_id = :run_id
                          AND source_partition_key = :source_index
                          AND stream_kind = :stream_kind
                        """
                    ),
                    {
                        "checkpoint_status": "PENDING" if retrying else "FAILED",
                        "error_code": payload.get("error_code"),
                        "error_message": payload.get("error_message"),
                        "run_id": run_id,
                        "source_index": source_index,
                        "stream_kind": stream_kind,
                    },
                )
                if retrying:
                    retry_payload = {
                        "message_version": 1,
                        "operation_type": "ELASTICSEARCH_STANDARDIZE",
                        "run_id": str(run_id),
                        "dataset_id": int(checkpoint_values["dataset_id"]),
                        "source_index": source_index,
                        "stream_kind": stream_kind,
                    }
                    await session.execute(
                        text(
                            """
                            INSERT INTO control.standardization_outbox_event (
                                workspace_id, run_id, source_partition_key,
                                stream_kind, event_type, deduplication_key,
                                payload_json
                            ) VALUES (
                                :workspace_id, :run_id, :source_index,
                                :stream_kind, 'standardization.partition.dispatch',
                                :deduplication_key, CAST(:payload AS jsonb)
                            )
                            ON CONFLICT (deduplication_key) DO NOTHING
                            """
                        ),
                        {
                            "workspace_id": checkpoint_values["workspace_id"],
                            "run_id": run_id,
                            "source_index": source_index,
                            "stream_kind": stream_kind,
                            "deduplication_key": (
                                f"standardize-retry:{run_id}:{source_index}:"
                                f"{stream_kind}:{attempt + 1}"
                            ),
                            "payload": json.dumps(retry_payload),
                        },
                    )
        else:
            await session.execute(
                text(
                    """
                    UPDATE control.standardization_checkpoint
                    SET checkpoint_status = 'COMPLETE',
                        completed_at = COALESCE(completed_at, now()),
                        scanned_record_count = GREATEST(scanned_record_count, :scanned),
                        new_record_count = GREATEST(new_record_count, :new_records),
                        duplicate_record_count = GREATEST(duplicate_record_count, :duplicates),
                        rejected_record_count = GREATEST(rejected_record_count, :rejected),
                        quarantined_record_count = GREATEST(quarantined_record_count, :quarantined),
                        updated_at = now()
                    WHERE run_id = :run_id
                      AND source_partition_key = :source_index
                      AND stream_kind = :stream_kind
                    """
                ),
                {
                    "scanned": int(metrics.get("scanned", 0)),
                    "new_records": int(metrics.get("new", 0)),
                    "duplicates": int(metrics.get("duplicates", 0)),
                    "rejected": int(metrics.get("rejected", 0)),
                    "quarantined": int(metrics.get("quarantined", 0)),
                    "run_id": run_id,
                    "source_index": source_index,
                    "stream_kind": stream_kind,
                },
            )

        await _finalize_run_if_terminal(session, run_id)
        await session.commit()


async def _finalize_run_if_terminal(
    session: AsyncSession, run_id: uuid.UUID
) -> bool:
    aggregate = (
            await session.execute(
                text(
                    """
                    SELECT count(*) FILTER (
                               WHERE checkpoint_status NOT IN ('COMPLETE','FAILED')
                           ) AS active,
                           count(*) FILTER (WHERE checkpoint_status = 'FAILED') AS failed,
                           coalesce(sum(scanned_record_count), 0) AS scanned,
                           coalesce(sum(new_record_count), 0) AS new_records,
                           coalesce(sum(duplicate_record_count), 0) AS duplicates,
                           coalesce(sum(rejected_record_count), 0) AS rejected,
                           coalesce(sum(quarantined_record_count), 0) AS quarantined
                    FROM control.standardization_checkpoint
                    WHERE run_id = :run_id
                    """
                ),
                {"run_id": run_id},
            )
    ).one()
    values = aggregate._mapping
    if int(values["active"]) != 0:
        return False
    failed = int(values["failed"])
    warning_count = int(values["rejected"]) + int(values["quarantined"])
    run_status = (
        "FAILED"
        if failed
        else "SUCCEEDED_WITH_WARNINGS"
        if warning_count
        else "SUCCEEDED"
    )
    await session.execute(
        text(
            """
            UPDATE control.standardization_run
            SET run_status = :run_status, source_record_count = :scanned,
                new_record_count = :new_records,
                duplicate_record_count = :duplicates,
                accepted_record_count = :new_records,
                rejected_record_count = :rejected,
                quarantined_record_count = :quarantined,
                completed_at = COALESCE(completed_at, now()), updated_at = now(),
                error_code = CASE WHEN :failed > 0 THEN 'PARTITION_FAILED' ELSE NULL END
            WHERE run_id = :run_id
              AND run_status IN ('PENDING','RUNNING')
            """
        ),
        {
            "run_status": run_status,
            "scanned": int(values["scanned"]),
            "new_records": int(values["new_records"]),
            "duplicates": int(values["duplicates"]),
            "rejected": int(values["rejected"]),
            "quarantined": int(values["quarantined"]),
            "failed": failed,
            "run_id": run_id,
        },
    )
    return True


async def reconcile_partition_once(
    session_factory: async_sessionmaker[AsyncSession], stale_seconds: int
) -> bool:
    async with session_factory() as session:
        checkpoint = (
            await session.execute(
                text(
                    """
                    SELECT checkpoint.run_id, checkpoint.dataset_id,
                           checkpoint.source_partition_key,
                           checkpoint.stream_kind, checkpoint.attempt,
                           checkpoint.checkpoint_status, run.workspace_id
                    FROM control.standardization_checkpoint AS checkpoint
                    JOIN control.standardization_run AS run USING (run_id)
                    WHERE run.run_status IN ('PENDING','RUNNING')
                      AND (
                          checkpoint.checkpoint_status = 'FAILED'
                          OR (
                              checkpoint.checkpoint_status = 'RUNNING'
                              AND checkpoint.updated_at <=
                                  now() - make_interval(secs => :stale_seconds)
                          )
                      )
                    ORDER BY checkpoint.updated_at
                    LIMIT 1
                    FOR UPDATE OF checkpoint SKIP LOCKED
                    """
                ),
                {"stale_seconds": stale_seconds},
            )
        ).first()
        if checkpoint is not None:
            values = checkpoint._mapping
            attempt = int(values["attempt"])
            retrying = attempt < 3
            await session.execute(
                text(
                    """
                    UPDATE control.standardization_checkpoint
                    SET checkpoint_status = :checkpoint_status,
                        error_code = CASE
                            WHEN checkpoint_status = 'RUNNING' THEN 'WORKER_STALE'
                            ELSE error_code
                        END,
                        error_message = CASE
                            WHEN checkpoint_status = 'RUNNING'
                                THEN 'worker stopped reporting progress before result'
                            ELSE error_message
                        END,
                        updated_at = now()
                    WHERE run_id = :run_id
                      AND source_partition_key = :source_index
                      AND stream_kind = :stream_kind
                    """
                ),
                {
                    "checkpoint_status": "PENDING" if retrying else "FAILED",
                    "run_id": values["run_id"],
                    "source_index": values["source_partition_key"],
                    "stream_kind": values["stream_kind"],
                },
            )
            if retrying:
                retry_payload = {
                    "message_version": 1,
                    "operation_type": "ELASTICSEARCH_STANDARDIZE",
                    "run_id": str(values["run_id"]),
                    "dataset_id": int(values["dataset_id"]),
                    "source_index": values["source_partition_key"],
                    "stream_kind": values["stream_kind"],
                }
                await session.execute(
                    text(
                        """
                        INSERT INTO control.standardization_outbox_event (
                            workspace_id, run_id, source_partition_key,
                            stream_kind, event_type, deduplication_key,
                            payload_json
                        ) VALUES (
                            :workspace_id, :run_id, :source_index,
                            :stream_kind, 'standardization.partition.dispatch',
                            :deduplication_key, CAST(:payload AS jsonb)
                        )
                        ON CONFLICT (deduplication_key) DO NOTHING
                        """
                    ),
                    {
                        "workspace_id": values["workspace_id"],
                        "run_id": values["run_id"],
                        "source_index": values["source_partition_key"],
                        "stream_kind": values["stream_kind"],
                        "deduplication_key": (
                            f"standardize-retry:{values['run_id']}:"
                            f"{values['source_partition_key']}:"
                            f"{values['stream_kind']}:{attempt + 1}"
                        ),
                        "payload": json.dumps(retry_payload),
                    },
                )
            await _finalize_run_if_terminal(session, values["run_id"])
            await session.commit()
            return True

        finalizable_run = (
            await session.execute(
                text(
                    """
                    SELECT run.run_id
                    FROM control.standardization_run AS run
                    WHERE run.run_status IN ('PENDING','RUNNING')
                      AND EXISTS (
                          SELECT 1
                          FROM control.standardization_checkpoint AS checkpoint
                          WHERE checkpoint.run_id = run.run_id
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM control.standardization_checkpoint AS checkpoint
                          WHERE checkpoint.run_id = run.run_id
                            AND checkpoint.checkpoint_status
                                NOT IN ('COMPLETE','FAILED')
                      )
                    ORDER BY run.created_at
                    LIMIT 1
                    FOR UPDATE OF run SKIP LOCKED
                    """
                )
            )
        ).first()
        if finalizable_run is None:
            return False
        await _finalize_run_if_terminal(session, finalizable_run._mapping["run_id"])
        await session.commit()
        return True


async def _consume_queue(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    queue_url: str,
    handler: Callable[[dict[str, Any]], Awaitable[None]],
) -> None:
    sqs = boto3.client("sqs", region_name=settings.aws_region)
    while True:
        response = await asyncio.to_thread(
            sqs.receive_message,
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,
            VisibilityTimeout=120,
        )
        for message in response.get("Messages", []):
            try:
                await handler(json.loads(message["Body"]))
            except Exception:
                LOGGER.exception("standardization result message failed")
                continue
            await asyncio.to_thread(
                sqs.delete_message,
                QueueUrl=queue_url,
                ReceiptHandle=message["ReceiptHandle"],
            )


async def run_worker(mode: str) -> None:
    settings = get_settings()
    engine = create_async_engine(await resolve_database_url(settings), pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        if mode == "outbox":
            while True:
                reconciled = await reconcile_partition_once(
                    session_factory,
                    settings.standardization_stale_partition_seconds,
                )
                processed = await process_outbox_once(session_factory, settings)
                if not processed and not reconciled:
                    await asyncio.sleep(settings.standardization_worker_poll_seconds)
        elif mode == "result":
            if not settings.standardization_result_queue_url:
                raise RuntimeError("STANDARDIZATION_RESULT_QUEUE_URL is not configured")

            async def result_handler(payload: dict[str, Any]) -> None:
                await process_result_message(session_factory, payload)

            await _consume_queue(
                settings,
                session_factory,
                settings.standardization_result_queue_url,
                result_handler,
            )
        else:
            raise ValueError(f"unsupported worker mode: {mode}")
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Standardization control-plane workers")
    parser.add_argument("mode", choices=["outbox", "result"])
    args = parser.parse_args()
    logging.basicConfig(level=get_settings().log_level)
    asyncio.run(run_worker(args.mode))


if __name__ == "__main__":
    main()
