import uuid
from collections.abc import Mapping
from typing import Any

import pytest

from app.core.config import Settings
from app.workers.standardization import (
    process_outbox_once,
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

    def begin(self) -> "_Session":
        return self

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


async def test_outbox_dispatch_selects_legacy_run_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _Session([_Result()])

    async def forbidden_dispatch(*_args: object) -> None:
        pytest.fail("2026 outbox must not dispatch")

    monkeypatch.setattr("app.workers.standardization._publish_dispatch", forbidden_dispatch)
    settings = Settings(_env_file=None, standardization_legacy_scope_enabled=True)

    processed = await process_outbox_once(_SessionFactory(session), settings)  # type: ignore[arg-type]

    assert processed is False
    sql = session.calls[0][0]
    assert "JOIN control.standardization_run AS run" in sql
    assert "run.run_scope = 'legacy'" in sql
    assert session.commits == 0


async def test_outbox_still_dispatches_2025_legacy_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = {"run_id": str(RUN_ID), "operation_type": "ELASTICSEARCH_STANDARDIZE"}
    session = _Session([
        _Result(first={"outbox_event_id": RUN_ID, "attempt": 0, "payload_json": payload}),
        _Result(),
        _Result(),
        _Result(),
    ])
    sent: list[dict[str, Any]] = []

    async def dispatch(_settings: Settings, message: dict[str, Any]) -> None:
        sent.append(message)

    monkeypatch.setattr("app.workers.standardization._publish_dispatch", dispatch)
    settings = Settings(_env_file=None, standardization_legacy_scope_enabled=True)

    processed = await process_outbox_once(_SessionFactory(session), settings)  # type: ignore[arg-type]

    assert processed is True
    assert sent == [payload]
    assert "run_scope = 'legacy'" in session.calls[-1][0]
    assert session.commits == 1


@pytest.mark.parametrize("status", ["SUCCEEDED", "FAILED"])
async def test_delayed_baseline_2026_result_cannot_mutate_checkpoint(status: str) -> None:
    session = _Session([_Result()])
    payload = {**_failure_payload(), "status": status}

    with pytest.raises(RuntimeError, match="legacy standardization run not found"):
        await process_result_message(
            _SessionFactory(session), payload, legacy_scope_enabled=True  # type: ignore[arg-type]
        )

    assert "run_scope = 'legacy'" in session.calls[0][0]
    assert len(session.calls) == 1
    assert session.commits == 0


@pytest.mark.parametrize("status", ["SUCCEEDED", "FAILED"])
async def test_result_rejects_null_workspace_before_scope_migration(status: str) -> None:
    session = _Session([_Result()])

    with pytest.raises(RuntimeError, match="legacy standardization run not found"):
        await process_result_message(
            _SessionFactory(session), {**_failure_payload(), "status": status}  # type: ignore[arg-type]
        )

    assert "run.workspace_id IS NOT NULL" in session.calls[0][0]
    assert "run_scope" not in session.calls[0][0]
    assert len(session.calls) == 1
    assert session.commits == 0


async def test_2025_success_result_finalizes_with_legacy_scope_enabled() -> None:
    session = _Session([
        _Result(first={"allowed": 1}),
        _Result(),
        _Result(one=_aggregate(active=0, failed=0)),
        _Result(),
    ])
    payload = {**_failure_payload(), "status": "SUCCEEDED"}

    await process_result_message(
        _SessionFactory(session), payload, legacy_scope_enabled=True  # type: ignore[arg-type]
    )

    assert "run.run_scope = 'legacy'" in session.calls[0][0]
    assert "run_scope = 'legacy'" in session.calls[-1][0]
    assert session.calls[-1][1]["run_status"] == "SUCCEEDED_WITH_WARNINGS"
    assert session.commits == 1


async def test_reconciler_scopes_both_selections_to_legacy() -> None:
    session = _Session([_Result(), _Result()])

    reconciled = await reconcile_partition_once(
        _SessionFactory(session), 900, legacy_scope_enabled=True  # type: ignore[arg-type]
    )

    assert reconciled is False
    assert len(session.calls) == 2
    assert all("run.run_scope = 'legacy'" in sql for sql, _ in session.calls)


async def test_pre_migration_workers_do_not_reference_run_scope() -> None:
    session = _Session([_Result(), _Result()])

    await reconcile_partition_once(_SessionFactory(session), 900)  # type: ignore[arg-type]

    assert all("run_scope" not in sql for sql, _ in session.calls)


async def test_pre_migration_outbox_does_not_reference_run_scope() -> None:
    session = _Session([_Result()])
    settings = Settings(_env_file=None, standardization_legacy_scope_enabled=False)

    processed = await process_outbox_once(_SessionFactory(session), settings)  # type: ignore[arg-type]

    assert processed is False
    assert "run_scope" not in session.calls[0][0]
    assert "run.workspace_id IS NOT NULL" in session.calls[0][0]


@pytest.mark.parametrize(("attempt", "next_attempt"), [(1, 2), (2, 3)])
async def test_failed_attempt_requeues_partition(
    attempt: int, next_attempt: int
) -> None:
    session = _Session(
        [
            _Result(first={"allowed": 1}),
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
            _Result(first={"allowed": 1}),
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
            _Result(first={"allowed": 1}),
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
            _Result(first={"allowed": 1}),
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
