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

Run background processes separately from API replicas:

```bash
uv run python -m app.workers.connectors outbox
uv run python -m app.workers.connectors occurrence
uv run python -m app.workers.connectors result
```

The CDK output values map to `CONNECTOR_DISPATCH_QUEUE_URL`,
`CONNECTOR_OCCURRENCE_QUEUE_URL`, `CONNECTOR_OCCURRENCE_DLQ_ARN`,
`CONNECTOR_RESULT_QUEUE_URL`, `CONNECTOR_SCHEDULER_GROUP`, and
`CONNECTOR_SCHEDULER_ROLE_ARN`. Set `CONNECTOR_RUNTIME_WORKSPACE_ID` to limit the
single-workspace POC runtime while retaining workspace-scoped metadata APIs.

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
