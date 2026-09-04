from __future__ import annotations

import argparse
import asyncio
import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import boto3
from botocore.exceptions import ClientError
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import Settings, get_settings
from app.infrastructure.secrets.aws import resolve_database_url
from app.modules.connectors.model import (
    ConnectorOperation,
    ConnectorSchedule,
    OutboxEvent,
    ScheduleOccurrence,
    SyncRun,
)
from app.modules.connectors.service import ActiveSyncRunError, materialize_sync_run

LOGGER = logging.getLogger(__name__)
FAST_OPERATION_TYPES = frozenset(
    {"CONNECTION_TEST", "SCHEMA_DISCOVERY", "DATASET_PREVIEW"}
)
PREVIEW_CLEANUP_INTERVAL_SECONDS = 60.0


async def cleanup_expired_preview_results(session: AsyncSession) -> None:
    await session.execute(
        update(ConnectorOperation)
        .where(
            ConnectorOperation.operation_type == "DATASET_PREVIEW",
            ConnectorOperation.result_expires_at.is_not(None),
            ConnectorOperation.result_expires_at <= datetime.now(UTC),
        )
        .values(result_json={"expired": True}, result_expires_at=None)
    )


def _scheduler_expression(timing: dict[str, Any]) -> str:
    hour, minute = str(timing["time"]).split(":", 1)
    frequency = timing["frequency"]
    if frequency == "daily":
        return f"cron({minute} {hour} * * ? *)"
    if frequency == "weekly":
        days = ",".join(timing["days_of_week"])
        return f"cron({minute} {hour} ? * {days} *)"
    if frequency == "monthly":
        return f"cron({minute} {hour} {int(timing['day_of_month'])} * ? *)"
    raise ValueError(f"unsupported schedule frequency: {frequency}")


def _scheduler_bound(value: date, timezone: str, *, end_of_day: bool) -> datetime:
    local_time = time.max if end_of_day else time.min
    return datetime.combine(value, local_time, tzinfo=ZoneInfo(timezone)).astimezone(UTC)


def _scheduler_start_bound(
    value: date,
    timing: dict[str, Any],
    timezone: str,
    *,
    now: datetime | None = None,
) -> datetime | None:
    hour, minute = (int(part) for part in str(timing["time"]).split(":", 1))
    start = datetime.combine(
        value,
        time(hour=hour, minute=minute),
        tzinfo=ZoneInfo(timezone),
    ).astimezone(UTC)
    return start if start > (now or datetime.now(UTC)) else None


async def _queue_arn(settings: Settings, queue_url: str) -> str:
    client = boto3.client("sqs", region_name=settings.aws_region)
    response = await asyncio.to_thread(
        client.get_queue_attributes, QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )
    return str(response["Attributes"]["QueueArn"])


async def _publish_dispatch(settings: Settings, payload: dict[str, Any]) -> None:
    operation_type = payload.get("operation_type")
    use_fast_queue = (
        settings.connector_fast_operations_enabled
        and operation_type in FAST_OPERATION_TYPES
    )
    if use_fast_queue and not settings.connector_fast_operation_queue_url:
        raise RuntimeError(
            "CONNECTOR_FAST_OPERATION_QUEUE_URL is required when "
            "CONNECTOR_FAST_OPERATIONS_ENABLED is enabled"
        )
    if not use_fast_queue and not settings.connector_dispatch_queue_url:
        raise RuntimeError("CONNECTOR_DISPATCH_QUEUE_URL is not configured")

    queue_url = (
        settings.connector_fast_operation_queue_url
        if use_fast_queue
        else settings.connector_dispatch_queue_url
    )
    message: dict[str, Any] = {
        "QueueUrl": queue_url,
        "MessageBody": json.dumps(payload, separators=(",", ":")),
    }
    client = boto3.client("sqs", region_name=settings.aws_region)
    await asyncio.to_thread(client.send_message, **message)


async def _mark_operation_dispatched(
    session: AsyncSession, operation_id: uuid.UUID
) -> None:
    await session.execute(
        update(ConnectorOperation)
        .where(
            ConnectorOperation.operation_id == operation_id,
            ConnectorOperation.status == "QUEUED",
        )
        .values(status="DISPATCHED")
    )


async def _sync_aws_schedule(
    session: AsyncSession, settings: Settings, event: OutboxEvent
) -> None:
    if not settings.connector_scheduler_group:
        raise RuntimeError("CONNECTOR_SCHEDULER_GROUP is not configured")
    if not settings.connector_scheduler_role_arn:
        raise RuntimeError("CONNECTOR_SCHEDULER_ROLE_ARN is not configured")
    if not settings.connector_occurrence_queue_url:
        raise RuntimeError("CONNECTOR_OCCURRENCE_QUEUE_URL is not configured")
    payload = event.payload_json
    schedule = await session.get(ConnectorSchedule, uuid.UUID(payload["schedule_id"]))
    if schedule is None:
        raise RuntimeError("schedule not found")
    client = boto3.client("scheduler", region_name=settings.aws_region)
    schedule_name = schedule.aws_schedule_name or f"connector-{schedule.schedule_id.hex}"
    action = payload.get("action")
    if action in {"pause", "resume"}:
        desired_state = "DISABLED" if action == "pause" else "ENABLED"
        current = await asyncio.to_thread(
            client.get_schedule,
            GroupName=settings.connector_scheduler_group,
            Name=schedule_name,
        )
        await asyncio.to_thread(
            client.update_schedule,
            GroupName=settings.connector_scheduler_group,
            Name=schedule_name,
            ScheduleExpression=current["ScheduleExpression"],
            ScheduleExpressionTimezone=current.get("ScheduleExpressionTimezone", schedule.timezone),
            FlexibleTimeWindow=current["FlexibleTimeWindow"],
            Target=current["Target"],
            State=desired_state,
            **({"StartDate": current["StartDate"]} if current.get("StartDate") else {}),
            **({"EndDate": current["EndDate"]} if current.get("EndDate") else {}),
        )
        schedule.status = "PAUSED" if action == "pause" else "ACTIVE"
        return

    queue_arn = await _queue_arn(settings, settings.connector_occurrence_queue_url)
    target_input = json.dumps(
        {
            "message_version": 1,
            "schedule_id": str(schedule.schedule_id),
            "workspace_id": str(schedule.workspace_id),
            "dataset_id": str(schedule.dataset_id),
            "occurrence_key": "<aws.scheduler.execution-id>",
            "scheduled_at": "<aws.scheduler.scheduled-time>",
        },
        separators=(",", ":"),
    )
    request: dict[str, Any] = {
        "GroupName": settings.connector_scheduler_group,
        "Name": schedule_name,
        "ScheduleExpression": _scheduler_expression(schedule.timing_json),
        "ScheduleExpressionTimezone": schedule.timezone,
        "FlexibleTimeWindow": {"Mode": "OFF"},
        "State": "ENABLED" if payload.get("activate", True) else "DISABLED",
        "Target": {
            "Arn": queue_arn,
            "RoleArn": settings.connector_scheduler_role_arn,
            "Input": target_input,
            "DeadLetterConfig": {"Arn": settings.connector_occurrence_dlq_arn},
            "RetryPolicy": {"MaximumEventAgeInSeconds": 3600, "MaximumRetryAttempts": 3},
        },
        **(
            {
                "EndDate": _scheduler_bound(
                    schedule.end_date, schedule.timezone, end_of_day=True
                )
            }
            if schedule.end_date
            else {}
        ),
    }
    if schedule.start_date:
        start_date = _scheduler_start_bound(
            schedule.start_date,
            schedule.timing_json,
            schedule.timezone,
        )
        if start_date:
            request["StartDate"] = start_date
    if not settings.connector_occurrence_dlq_arn:
        request["Target"].pop("DeadLetterConfig")
    try:
        response = await asyncio.to_thread(client.create_schedule, **request)
        schedule.aws_schedule_arn = str(response["ScheduleArn"])
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "ConflictException":
            raise
        response = await asyncio.to_thread(client.update_schedule, **request)
        schedule.aws_schedule_arn = str(response["ScheduleArn"])
    schedule.aws_schedule_name = schedule_name
    schedule.status = "ACTIVE" if payload.get("activate", True) else "PAUSED"
    schedule.last_error_code = None


async def process_outbox_once(
    session_factory: async_sessionmaker[AsyncSession], settings: Settings
) -> bool:
    async with session_factory() as session:
        async with session.begin():
            event = await session.scalar(
                select(OutboxEvent)
                .where(
                    OutboxEvent.status.in_(["PENDING", "FAILED", "PROCESSING"]),
                    OutboxEvent.available_at <= datetime.now(UTC),
                )
                .order_by(OutboxEvent.created_at)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if event is None:
                return False
            event.status = "PROCESSING"
            event.attempt += 1
            event.available_at = datetime.now(UTC) + timedelta(minutes=5)
        try:
            if event.event_type == "operation.dispatch":
                await _publish_dispatch(settings, event.payload_json)
                await _mark_operation_dispatched(
                    session, uuid.UUID(event.payload_json["operation_id"])
                )
            elif event.event_type.startswith("schedule."):
                await _sync_aws_schedule(session, settings, event)
            else:
                raise RuntimeError(f"unsupported outbox event: {event.event_type}")
            event.status = "PROCESSED"
            event.processed_at = datetime.now(UTC)
            event.last_error = None
            await session.commit()
        except Exception as exc:
            event.status = "FAILED"
            event.last_error = type(exc).__name__
            event.available_at = datetime.now(UTC) + timedelta(
                seconds=min(300, 2 ** min(event.attempt, 8))
            )
            await session.commit()
            LOGGER.exception("connector outbox event failed id=%s", event.outbox_event_id)
        return True


async def process_occurrence_message(
    session_factory: async_sessionmaker[AsyncSession], settings: Settings, payload: dict[str, Any]
) -> None:
    schedule_id = uuid.UUID(payload["schedule_id"])
    workspace_id = uuid.UUID(payload["workspace_id"])
    dataset_id = uuid.UUID(payload["dataset_id"])
    occurrence_key = str(payload["occurrence_key"])
    scheduled_at = datetime.fromisoformat(str(payload["scheduled_at"]).replace("Z", "+00:00"))
    async with session_factory() as session:
        existing = await session.scalar(
            select(ScheduleOccurrence).where(
                ScheduleOccurrence.schedule_id == schedule_id,
                ScheduleOccurrence.occurrence_key == occurrence_key,
            )
        )
        if existing:
            return
        occurrence = ScheduleOccurrence(
            workspace_id=workspace_id,
            schedule_id=schedule_id,
            occurrence_key=occurrence_key,
            scheduled_at=scheduled_at,
            status="RECEIVED",
        )
        session.add(occurrence)
        await session.flush()
        try:
            operation, _ = await materialize_sync_run(
                session,
                settings,
                workspace_id=workspace_id,
                dataset_id=dataset_id,
                trigger_type="SCHEDULE",
                idempotency_key=f"schedule:{schedule_id}:{occurrence_key}",
                commit=False,
            )
            occurrence.operation_id = operation.operation_id
            occurrence.status = "QUEUED"
        except ActiveSyncRunError:
            occurrence.status = "SKIPPED_ACTIVE_RUN"
        await session.commit()


async def process_result_message(
    session_factory: async_sessionmaker[AsyncSession], payload: dict[str, Any]
) -> None:
    operation_id = uuid.UUID(payload["operation_id"])
    terminal_status = str(payload["status"])
    if terminal_status not in {"SUCCEEDED", "FAILED", "CANCELLED"}:
        raise ValueError("result message status must be terminal")
    async with session_factory() as session:
        operation = await session.scalar(
            select(ConnectorOperation)
            .where(ConnectorOperation.operation_id == operation_id)
            .with_for_update()
        )
        if operation is None:
            raise RuntimeError("operation not found")
        if operation.status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            return
        if (
            operation.operation_type in FAST_OPERATION_TYPES
            and operation.lease_owner is not None
            and operation.lease_expires_at is not None
            and operation.lease_expires_at > datetime.now(UTC)
        ):
            LOGGER.info(
                "ignoring legacy result for fast-leased connector operation id=%s",
                operation_id,
            )
            return
        operation.status = terminal_status
        operation.completed_at = datetime.now(UTC)
        operation.error_code = payload.get("error_code")
        operation.error_message = payload.get("error_message")
        operation.result_json = payload.get("metrics") or {}
        run = await session.scalar(select(SyncRun).where(SyncRun.operation_id == operation_id))
        if run:
            run.status = terminal_status
            run.completed_at = operation.completed_at
        await session.commit()


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
                LOGGER.exception("connector queue message failed")
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
            next_preview_cleanup_at = 0.0
            while True:
                now = asyncio.get_running_loop().time()
                if now >= next_preview_cleanup_at:
                    async with session_factory() as session:
                        await cleanup_expired_preview_results(session)
                        await session.commit()
                    next_preview_cleanup_at = now + PREVIEW_CLEANUP_INTERVAL_SECONDS
                processed = await process_outbox_once(session_factory, settings)
                if not processed:
                    await asyncio.sleep(settings.connector_worker_poll_seconds)
        elif mode == "occurrence":
            if not settings.connector_occurrence_queue_url:
                raise RuntimeError("CONNECTOR_OCCURRENCE_QUEUE_URL is not configured")
            async def occurrence_handler(payload: dict[str, Any]) -> None:
                await process_occurrence_message(session_factory, settings, payload)

            await _consume_queue(
                settings,
                session_factory,
                settings.connector_occurrence_queue_url,
                occurrence_handler,
            )
        elif mode == "result":
            if not settings.connector_result_queue_url:
                raise RuntimeError("CONNECTOR_RESULT_QUEUE_URL is not configured")
            async def result_handler(payload: dict[str, Any]) -> None:
                await process_result_message(session_factory, payload)

            await _consume_queue(
                settings,
                session_factory,
                settings.connector_result_queue_url,
                result_handler,
            )
        else:
            raise ValueError(f"unsupported worker mode: {mode}")
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Connector control-plane background workers")
    parser.add_argument("mode", choices=["outbox", "occurrence", "result"])
    args = parser.parse_args()
    logging.basicConfig(level=get_settings().log_level)
    asyncio.run(run_worker(args.mode))


if __name__ == "__main__":
    main()
