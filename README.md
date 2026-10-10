# Journey Builder API

Foundation for a modular FastAPI backend using async SQLAlchemy, PostgreSQL,
Redis, Pydantic Settings, Alembic, and AWS Secrets Manager.

## Architecture

The codebase uses a feature-first layered structure. It maps MVC concepts to an
API without forcing server-rendered web conventions:

- **Controller**: FastAPI routes and HTTP concerns.
- **Service**: use cases, transaction boundaries, and business rules.
- **Repository**: persistence queries for one feature/domain.
- **Model**: SQLAlchemy persistence entities.
- **Schema/View contract**: Pydantic request and response models.
- **Infrastructure**: shared PostgreSQL, Redis, and AWS adapters.

```text
app/
├── api/                         # Top-level route composition
├── common/repositories/         # Minimal reusable repository primitives
├── core/                        # Settings and logging
├── infrastructure/
│   ├── cache/                   # Redis client/cache adapter
│   ├── database/                # SQLAlchemy base/session lifecycle
│   └── secrets/                 # AWS Secrets Manager adapter
└── modules/
    ├── health/                  # Operational module
    └── <feature>/
        ├── controller.py
        ├── service.py
        ├── repository.py
        ├── model.py
        └── schemas.py
```

Each future feature owns its repository. Cross-feature orchestration belongs in
a service, not in a controller or a shared catch-all repository. Extract a
module into a separate deployable service only when its runtime or ownership
boundary is real; this layout keeps that move straightforward without starting
with premature microservices.

## Configuration precedence

The database URL is resolved once at process startup in this order:

1. `DATABASE_URL` (optional test/local override)
2. AWS Secrets Manager using `DATABASE_SECRET_ID` or `SECRET_ID`

The Compose stack does **not** provision PostgreSQL. The API connects to the
existing PostgreSQL RDS instance described by the AWS secret.

AWS database secret JSON:

```json
{
  "host": "database.example.internal",
  "port": 5432,
  "username": "application_user",
  "password": "secret",
  "dbname": "postgres"
}
```

The app uses the default AWS credential chain (task role, pod identity, local
profile, or environment credentials). Do not place AWS credentials in this
repository. Secret values are converted with SQLAlchemy's URL builder so
special characters in passwords are escaped safely. Secret Manager is never
called per request.

## Local development

```bash
cp .env.example .env  # only if you do not already have one
uv sync
docker compose up redis -d
uv run alembic upgrade head
uv run uvicorn app.main:app --reload --port 8100
```

Endpoints:

- API docs: <http://localhost:8100/docs>
- Liveness: <http://localhost:8100/api/v1/health/live>
- Readiness: <http://localhost:8100/api/v1/health/ready>

### Database connector control plane

Forward-only `app_id` cutover: `POST /api/v1/workspaces` requires a **literal
decimal string** (for example `"00042"`, distinct from `"42"`). No connector
Bearer credential is required; `app_id` is passed through the existing connector
pipeline as in the upload path.

The sync requires a workspace app and binds it to its newly allocated
ingestion batch before dispatch. The connector worker and the database enforce
the workspace/connection/physical-source/batch lineage through raw, current,
membership, and staging history. Legacy connector sources are quarantined; do
not reactivate an old app-less schedule or reuse its physical source. File
upload `batch_ref` parsing and upload-only publication are separate contracts;
`CONNECTOR_PUBLICATION_STATUS_ENABLED` remains `false` until a connector
publication contract is reviewed.

When the ECS fast-operation service is scaled to zero, set
`CONNECTOR_FAST_OPERATIONS_ENABLED=false` **on the outbox worker** so test,
discovery, and preview requests use the normal SQS dispatcher/Step Functions
path. This is a runtime setting, not an API-only flag. Verify both the queue
and the outbox container environment after rollout; otherwise those requests
are accepted but never consumed. The normal dispatch mapping must be Enabled.

The connector module owns workspace-scoped PostgreSQL metadata and runtime
state for PostgreSQL, MySQL, MariaDB, and MongoDB sources. Credentials are
write-only API inputs; only their Secrets Manager ARN is stored in PostgreSQL.
Connection test and discovery are asynchronous operations. Dataset sync creates
the existing CDP landing/ingestion/stitching lineage before dispatch, and a
partial unique index enforces skip-overlap per dataset.

Primary routes are under `/api/v1/workspaces/{workspace_id}`:

- `POST /connectors`, `PUT /connectors/{connection_id}/credentials`
- `POST /connectors/{connection_id}/test` and `/discover`
- `PATCH /datasets/{dataset_id}`, schema snapshots, mapping create/publish routes
- `POST /datasets/{dataset_id}/preview` for an asynchronous, bounded raw-row preview
- schedule create/read/pause/resume and `POST /datasets/{dataset_id}/runs`
- operation and sync-run status reads

Dataset discovery records an approximate row count without running a full
`COUNT(*)`. Preview operations default to 10 rows (maximum 50); poll the normal
operation endpoint and consume `result_json` before its 15-minute expiry.

Source credentials may include an optional `tunnel` object for SSH forwarding.
SSH authentication supports exactly one of `private_key` or `password`, plus an
optional `private_key_passphrase`. `host_key` accepts a known-hosts key line and
should be supplied outside disposable POC environments. A tunneled source must
use one database `host`; URI and multi-host connection forms are rejected.

Run background processes separately from API replicas:

```bash
uv run python -m app.workers.connectors outbox
uv run python -m app.workers.connectors occurrence
uv run python -m app.workers.connectors result
```

The CDK output values map to `CONNECTOR_DISPATCH_QUEUE_URL`,
`CONNECTOR_OCCURRENCE_QUEUE_URL`, `CONNECTOR_OCCURRENCE_DLQ_ARN`,
`CONNECTOR_RESULT_QUEUE_URL`, `CONNECTOR_SCHEDULER_GROUP`, and
`CONNECTOR_SCHEDULER_ROLE_ARN`. Set `CONNECTOR_FAST_OPERATIONS_ENABLED=true` and
`CONNECTOR_FAST_OPERATION_QUEUE_URL` to route connection tests, schema discovery,
and dataset previews to a standard (non-FIFO) fast SQS queue. FIFO queue URLs are
rejected because fast-operation sends intentionally omit FIFO-only message
attributes. Dataset syncs continue to use `CONNECTOR_DISPATCH_QUEUE_URL`. Set
`CONNECTOR_RUNTIME_WORKSPACE_ID` to limit the single-workspace POC runtime while
retaining workspace-scoped metadata APIs. The connector worker polling interval
defaults to one second and remains configurable with `CONNECTOR_WORKER_POLL_SECONDS`.

To roll back fast routing safely, first set
`CONNECTOR_FAST_OPERATIONS_ENABLED=false` so new operations return to the normal
dispatch queue. Keep the warm fast-queue consumer running until the fast queue is
drained before stopping it. The worker's atomic operation claim prevents duplicate
execution while both consumers are running, and the API ignores legacy results for
fast operations with an active worker lease. The lease guard is additional
protection, not a replacement for the drain-and-toggle sequence.

### Customer publication status cutover

`CONNECTOR_PUBLICATION_STATUS_ENABLED` defaults to `false`. In that compatibility
mode, connector status and successful-record totals retain their existing
stitching/workflow meaning; **this is not evidence of ClickHouse publication**.

Activation requires an explicitly approved deployment, in this order:

1. Apply `aws-flex-deploy/database/migrations/024_customer_identity_publication.sql`
   and `025_customer_publication_status.sql` to the **same PostgreSQL database**
   used by this API. Migration 025 also requires the existing pipeline view;
   it preserves its OID, ACL, column order and appended history fields.
2. As the API database role, verify `USAGE` on schema `control` and `SELECT` on
   `control.customer_publication_status`. Migration 025 copies existing pipeline
   view SELECT grants; provision explicit least-privilege grants if that role was
   not an existing reader. Read `SELECT * FROM control.customer_publication_status
   LIMIT 0` as that role before enabling. Do not grant transfer-table writes.
3. Configure **every API replica** with `CONNECTOR_PUBLICATION_STATUS_ENABLED=true`
   and restart/roll out those replicas (settings are process-cached). Enable this
   only as part of the approved publication-worker/registry cutover. Poll a known
   sync operation and run, and check connector-list totals before declaring the
   cutover active. No frontend or upload request changes are required.

When enabled, GET operation/run and POST run responses (including idempotent
replays) use the same publication projection. Internal sync `SUCCEEDED` becomes
`TRANSFER_PENDING`, `TRANSFERRING`, `TRANSFER_FAILED`, `TRANSFER_BLOCKED`, or final
`COMPLETED` / `COMPLETED_WITH_WARNINGS` based on the latest stitching attempt for
the ingestion batch. Valid post-ingestion stitching state also supersedes stale
execution state, including an originally `FAILED` run recovered by a later retry.
Missing lineage or unfinished ingestion does not hide a genuine pre-stitch failure.
A missing outbox never completes; an older published attempt cannot complete a
newer retry. Non-sync operations and stored execution statuses/errors are unchanged.
Only published runs contribute to connector-list `successful_records`, including
recovered executions; ingestion rejection/duplicate counters keep their existing
execution-stage meaning. Listing uses one bulk aggregate, not one lookup per connector.
Operation `updated_at` is the greatest of its operational timestamp and the effective
transfer's `updated_at`; it advances with publication without rewriting stored rows.
If capture fails before an outbox exists, migration 025 projects the latest succeeded
job's `customer_identity_capture_issue` as `TRANSFER_BLOCKED`, including its sanitized
reason and creation clock. A valid outbox always supersedes that journal, including
`PUBLISHED`. Operation/run responses expose the reason as nullable
`transfer_error_code`, separate from the unchanged execution-stage `error_code`.

With the flag enabled, missing migration/view or insufficient SELECT privileges
cause status/summary requests requiring the contract to fail (HTTP 500); there is
**no fallback to `SUCCEEDED`**. Treat that as an incomplete deployment and repair
schema/grants. Disabling the flag intentionally restores legacy semantics and is
not a safe way to claim publication completion. Existing `completed_at` fields
remain workflow timestamps, not ClickHouse publication timestamps.

Local regression tests (disposable DB only, run serially with the SQL tests):

```bash
CONNECTOR_PUBLICATION_TEST_URL=postgresql+asyncpg://postgres@127.0.0.1:61577/poststitch_status_test \
  uv run pytest tests/test_connector_publication_status.py
```

The PostgreSQL tests require the sibling `aws-flex-deploy` checkout for migration
025. Without that local-test environment variable, they skip rather than touch a
configured application database. HTTP/API behavior is tested; this does not claim
frontend or production verification.

### Manual Elasticsearch standardization

`POST /api/v1/workspaces/{workspace_id}/standardization-datasets/{dataset_id}/runs`
discovers every populated source index and creates work only where its frozen
cutoff is ahead of the durable baseline/version watermark. A repeated click
returns the active run instead of duplicating work. Poll the workspace-scoped
`GET /standardization-runs/{run_id}` route for aggregate counts.

Run the dedicated control-plane workers separately from connector workers:

```bash
uv run python -m app.workers.standardization outbox
uv run python -m app.workers.standardization result
```

The outbox worker also reconciles partitions whose ECS task stopped without a
result message. Completed checkpoints finalize the run from durable counts;
failed or stale running checkpoints are dispatched again up to three worker
attempts. `STANDARDIZATION_STALE_PARTITION_SECONDS` defaults to 900 seconds.

Configure `STANDARDIZATION_SOURCE_SECRET_ID`,
`STANDARDIZATION_DISPATCH_QUEUE_URL`, and `STANDARDIZATION_RESULT_QUEUE_URL`
from the separate standardization runtime stack. There is no schedule or
restore-status trigger in v1.

`STANDARDIZATION_SOURCE_SSLMODE` defaults to `require`, which encrypts forwarded
or tunneled PostgreSQL connections without CA/hostname verification. Use
`verify-full` only with the database CA bundle installed and a hostname that
matches the certificate. Legacy `STANDARDIZATION_SOURCE_SSL=true` is accepted
and normalized to `require`.

Run the complete API, Redis, and control-plane worker stack:

```bash
docker compose build api
docker compose run --rm api alembic upgrade head
docker compose up -d
```

The Compose deployment runs five workers from the same API image with separate
commands: connector `outbox`, `occurrence`, and `result`, plus standardization
`outbox` and `result`. Worker services disable the image's HTTP health check
because they do not expose the API port. Inspect them with:

```bash
docker compose ps
docker compose logs -f \
  connector-outbox connector-occurrence connector-result \
  standardization-outbox standardization-result
```

Before starting the workers, `.env` must contain the CDK output values for
`CONNECTOR_DISPATCH_QUEUE_URL`, `CONNECTOR_OCCURRENCE_QUEUE_URL`,
`CONNECTOR_OCCURRENCE_DLQ_ARN`, `CONNECTOR_RESULT_QUEUE_URL`,
`CONNECTOR_SCHEDULER_GROUP`, `CONNECTOR_SCHEDULER_ROLE_ARN`,
`STANDARDIZATION_SOURCE_SECRET_ID`, `STANDARDIZATION_DISPATCH_QUEUE_URL`, and
`STANDARDIZATION_RESULT_QUEUE_URL`.
When fast connector operations are enabled, it must also contain
`CONNECTOR_FAST_OPERATION_QUEUE_URL`.
Start with one replica per worker mode; scale only after checking database and
SQS load.

The API container reads `AWS_REGION` and `SECRET_ID` from `.env`. In deployed
environments, provide AWS access through an IAM role (ECS task role, EKS pod
identity, or EC2 instance role). To migrate RDS without adding a permanent
migration service:

```bash
docker compose run --rm api alembic upgrade head
```

## Adding a feature

To assign a legacy connector workspace's missing app before its first sync, call
`PATCH /api/v1/workspaces/{workspace_id}` with `{"app_id":"00042"}`. The value
is a literal decimal string (leading zeros are retained). The response is the
updated workspace. Unknown workspaces return 404, invalid payloads return 422,
and assigning an app after any connector sync run returns 409. An existing
non-null app ID cannot be rebound, even before a run (database guard); repeating
the same app ID is idempotent. This endpoint does not register connector engines
or repair failed connector creation.

1. Create `app/modules/<feature>/` with controller, service, repository, model,
   and schemas as needed.
2. Extend `BaseRepository` only for shared CRUD; keep feature queries in that
   feature's repository.
3. Let the service own `commit()` and multi-repository transactions. Repository
   methods should not commit independently.
4. Import the model module in `alembic/env.py`, then run:
   `make migration name=create_<feature>_tables`.
5. Include the feature router from `app/api/router.py`.
6. Add service/repository tests before exposing the route.

## Checks

```bash
make test
make lint
make typecheck
docker compose config
```

For horizontally scaled deployments, run `alembic upgrade head` as a separate
release job, not once per API replica. Docker Compose intentionally contains
only the long-running `api` and `redis` services.
