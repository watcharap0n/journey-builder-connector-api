import uuid
from collections.abc import Mapping
from typing import Any

import pytest

from app.workers.standardization import (
    process_result_message,
    reconcile_partition_once,
)

RUN_ID = uuid.UUID("00000000-0000-4000-8000-000000000001")
WORKSPACE_ID = uuid.UUID("00000000-0000-4000-8000-000000000002")


class _Row:
    def __init__(self, values: Mapping[str, Any]) -> None:
        self._mapping = dict(values)


class _Result:
    def __init__(
        self,
        *,
        first: Mapping[str, Any] | None = None,
        one: Mapping[str, Any] | None = None,
    ) -> None:
        self._first = _Row(first) if first is not None else None
        self._one = _Row(one) if one is not None else None

    def first(self) -> _Row | None:
        return self._first

    def one(self) -> _Row:
        assert self._one is not None
        return self._one


class _Session:
    def __init__(self, results: list[_Result]) -> None:
        self.results = results
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.commits = 0

    async def __aenter__(self) -> "_Session":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def execute(
        self, statement: object, parameters: dict[str, Any] | None = None
    ) -> _Result:
        self.calls.append((str(statement), parameters or {}))
        assert self.results, f"unexpected SQL: {statement}"
        return self.results.pop(0)

    async def commit(self) -> None:
        self.commits += 1


class _SessionFactory:
    def __init__(self, session: _Session) -> None:
        self.session = session

    def __call__(self) -> _Session:
        return self.session


def _checkpoint(status: str, attempt: int) -> dict[str, Any]:
    return {
        "attempt": attempt,
        "checkpoint_status": status,
        "dataset_id": 7,
        "workspace_id": WORKSPACE_ID,
    }


def _aggregate(*, active: int, failed: int) -> dict[str, int]:
    return {
        "active": active,
        "failed": failed,
        "scanned": 11,
        "new_records": 8,
        "duplicates": 1,
        "rejected": 1,
        "quarantined": 1,
    }


def _failure_payload() -> dict[str, Any]:
    return {
        "run_id": str(RUN_ID),
        "source_index": "sales-2026",
        "stream_kind": "versions",
        "status": "FAILED",
        "error_code": "RuntimeError",
        "error_message": "boom",
    }


@pytest.mark.parametrize(("attempt", "next_attempt"), [(1, 2), (2, 3)])
async def test_failed_attempt_requeues_partition(
    attempt: int, next_attempt: int
) -> None:
    session = _Session(
        [
            _Result(first=_checkpoint("FAILED", attempt)),
            _Result(),
            _Result(),
            _Result(one=_aggregate(active=1, failed=0)),
        ]
    )

    await process_result_message(_SessionFactory(session), _failure_payload())  # type: ignore[arg-type]

    checkpoint_update = next(
        params
        for sql, params in session.calls
        if "UPDATE control.standardization_checkpoint" in sql
    )
    retry_insert = next(
        params
        for sql, params in session.calls
        if "INSERT INTO control.standardization_outbox_event" in sql
    )
    assert checkpoint_update["checkpoint_status"] == "PENDING"
    assert retry_insert["deduplication_key"].endswith(f":{next_attempt}")
    assert not any("UPDATE control.standardization_run" in sql for sql, _ in session.calls)
    assert session.commits == 1


async def test_third_failed_attempt_is_terminal() -> None:
    session = _Session(
        [
            _Result(first=_checkpoint("FAILED", 3)),
            _Result(),
            _Result(one=_aggregate(active=0, failed=1)),
            _Result(),
        ]
    )

    await process_result_message(_SessionFactory(session), _failure_payload())  # type: ignore[arg-type]

    checkpoint_update = next(
        params
        for sql, params in session.calls
        if "UPDATE control.standardization_checkpoint" in sql
    )
    run_update = next(
        params
        for sql, params in session.calls
        if "UPDATE control.standardization_run" in sql
    )
    assert checkpoint_update["checkpoint_status"] == "FAILED"
    assert run_update["run_status"] == "FAILED"
    assert not any(
        "INSERT INTO control.standardization_outbox_event" in sql
        for sql, _ in session.calls
    )


async def test_stale_failure_result_does_not_mutate_completed_checkpoint() -> None:
    session = _Session(
        [
            _Result(first=_checkpoint("COMPLETE", 2)),
            _Result(one=_aggregate(active=0, failed=0)),
            _Result(),
        ]
    )

    await process_result_message(_SessionFactory(session), _failure_payload())  # type: ignore[arg-type]

    assert not any(
        "UPDATE control.standardization_checkpoint" in sql for sql, _ in session.calls
    )
    assert not any(
        "INSERT INTO control.standardization_outbox_event" in sql
        for sql, _ in session.calls
    )


async def test_old_failure_result_does_not_interrupt_running_retry() -> None:
    session = _Session(
        [
            _Result(first=_checkpoint("RUNNING", 2)),
            _Result(one=_aggregate(active=1, failed=0)),
        ]
    )

    await process_result_message(_SessionFactory(session), _failure_payload())  # type: ignore[arg-type]

    assert not any(
        "UPDATE control.standardization_checkpoint" in sql for sql, _ in session.calls
    )
    assert not any(
        "INSERT INTO control.standardization_outbox_event" in sql
        for sql, _ in session.calls
    )
    assert not any("UPDATE control.standardization_run" in sql for sql, _ in session.calls)


async def test_reconciler_finalizes_complete_checkpoint_without_result_message() -> None:
    session = _Session(
        [
            _Result(),
            _Result(first={"run_id": RUN_ID}),
            _Result(one=_aggregate(active=0, failed=0)),
            _Result(),
        ]
    )

    reconciled = await reconcile_partition_once(
        _SessionFactory(session),  # type: ignore[arg-type]
        900,
    )

    run_update = next(
        params
        for sql, params in session.calls
        if "UPDATE control.standardization_run" in sql
    )
    reconcile_selects = [
        sql
        for sql, _params in session.calls
        if "FROM control.standardization_" in sql and "run.run_status" in sql
    ]
    assert reconcile_selects
    assert all("run.workspace_id IS NOT NULL" in sql for sql in reconcile_selects)
    assert reconciled is True
    assert run_update["run_status"] == "SUCCEEDED_WITH_WARNINGS"
    assert session.commits == 1


async def test_reconciler_requeues_stale_running_checkpoint() -> None:
    stale_checkpoint = {
        "run_id": RUN_ID,
        "dataset_id": 7,
        "source_partition_key": "sales-2026",
        "stream_kind": "versions",
        "attempt": 1,
        "checkpoint_status": "RUNNING",
        "workspace_id": WORKSPACE_ID,
    }
    session = _Session(
        [
            _Result(first=stale_checkpoint),
            _Result(),
            _Result(),
            _Result(one=_aggregate(active=1, failed=0)),
        ]
    )

    reconciled = await reconcile_partition_once(
        _SessionFactory(session),  # type: ignore[arg-type]
        900,
    )

    retry_insert = next(
        params
        for sql, params in session.calls
        if "INSERT INTO control.standardization_outbox_event" in sql
    )
    checkpoint_select = next(
        sql
        for sql, _params in session.calls
        if "FROM control.standardization_checkpoint AS checkpoint" in sql
    )
    assert "run.workspace_id IS NOT NULL" in checkpoint_select
    assert reconciled is True
    assert retry_insert["deduplication_key"].endswith(":2")
    assert session.commits == 1
