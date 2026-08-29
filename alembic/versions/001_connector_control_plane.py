"""Create connector control-plane schemas and tables.

Revision ID: 001_connector_control_plane
Revises:
Create Date: 2026-08-29
"""

from sqlalchemy import DDL

from alembic import op

revision = "001_connector_control_plane"
down_revision = None
branch_labels = None
depends_on = None


DDL_STATEMENTS = r"""
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS platform;
CREATE SCHEMA IF NOT EXISTS connector;

CREATE TABLE platform.workspace (
    workspace_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    slug varchar(63) NOT NULL UNIQUE CHECK (slug ~ '^[a-z0-9][a-z0-9-]{0,62}$'),
    name varchar(255) NOT NULL,
    is_active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE connector.definition (
    key varchar(32) PRIMARY KEY,
    family varchar(32) NOT NULL,
    adapter_key varchar(100) NOT NULL UNIQUE,
    capabilities jsonb NOT NULL DEFAULT '{}'::jsonb,
    is_active boolean NOT NULL DEFAULT true
);

INSERT INTO connector.definition (key, family, adapter_key, capabilities)
VALUES
    ('postgresql', 'relational', 'builtin.connector.postgresql.v1',
     '{"snapshot":true,"incremental":true,"cdc":false}'::jsonb),
    ('mysql', 'relational', 'builtin.connector.mysql.v1',
     '{"snapshot":true,"incremental":true,"cdc":false}'::jsonb),
    ('mariadb', 'relational', 'builtin.connector.mariadb.v1',
     '{"snapshot":true,"incremental":true,"cdc":false}'::jsonb),
    ('mongodb', 'document', 'builtin.connector.mongodb.v1',
     '{"snapshot":true,"incremental":true,"cdc":false,"nested_paths":true}'::jsonb)
ON CONFLICT (key) DO UPDATE
SET family = EXCLUDED.family,
    adapter_key = EXCLUDED.adapter_key,
    capabilities = EXCLUDED.capabilities,
    is_active = true;

CREATE TABLE connector.connection (
    connection_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES platform.workspace(workspace_id),
    definition_key varchar(32) NOT NULL REFERENCES connector.definition(key),
    name varchar(255) NOT NULL,
    source_code varchar(63) NOT NULL CHECK (source_code ~ '^[a-z0-9][a-z0-9_-]{0,62}$'),
    endpoint_label varchar(255),
    secret_arn text,
    source_system_id bigint REFERENCES control.source_system(source_system_id),
    safe_config jsonb NOT NULL DEFAULT '{}'::jsonb,
    status varchar(20) NOT NULL DEFAULT 'PROVISIONING'
        CHECK (status IN ('PROVISIONING','DRAFT','TESTING','READY','ERROR','DISABLED')),
    last_tested_at timestamptz,
    last_error_code varchar(100),
    disabled_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_connection_workspace_name UNIQUE (workspace_id, name),
    CONSTRAINT uq_connection_workspace_source UNIQUE (workspace_id, source_code)
);
CREATE INDEX ix_connection_workspace_status
    ON connector.connection(workspace_id, status);

CREATE TABLE connector.dataset (
    dataset_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES platform.workspace(workspace_id),
    connection_id uuid NOT NULL REFERENCES connector.connection(connection_id),
    catalog_name varchar(255),
    namespace_name varchar(255),
    object_name varchar(255) NOT NULL,
    target_entity varchar(32) NOT NULL DEFAULT 'CUSTOMER'
        CHECK (target_entity = 'CUSTOMER'),
    sync_mode varchar(32) NOT NULL DEFAULT 'FULL_SNAPSHOT'
        CHECK (sync_mode IN ('FULL_SNAPSHOT','INCREMENTAL')),
    primary_key_paths jsonb NOT NULL DEFAULT '[]'::jsonb,
    cursor_paths jsonb NOT NULL DEFAULT '[]'::jsonb,
    soft_delete_path varchar(500),
    active_mapping_version_id uuid,
    status varchar(20) NOT NULL DEFAULT 'DISCOVERED'
        CHECK (status IN ('DISCOVERED','MAPPED','ACTIVE','PAUSED','DISABLED')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_dataset_source_object
        UNIQUE NULLS NOT DISTINCT (connection_id, catalog_name, namespace_name, object_name)
);
CREATE INDEX ix_dataset_workspace_status ON connector.dataset(workspace_id, status);

CREATE TABLE connector.schema_snapshot (
    schema_snapshot_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES platform.workspace(workspace_id),
    dataset_id uuid NOT NULL REFERENCES connector.dataset(dataset_id),
    schema_hash varchar(64) NOT NULL,
    schema_json jsonb NOT NULL,
    compatibility_status varchar(32) NOT NULL DEFAULT 'CURRENT',
    discovered_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_schema_snapshot_hash UNIQUE (dataset_id, schema_hash)
);
CREATE INDEX ix_schema_snapshot_dataset_discovered
    ON connector.schema_snapshot(dataset_id, discovered_at DESC);

CREATE TABLE connector.mapping_version (
    mapping_version_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES platform.workspace(workspace_id),
    dataset_id uuid NOT NULL REFERENCES connector.dataset(dataset_id),
    version integer NOT NULL CHECK (version > 0),
    schema_snapshot_id uuid NOT NULL REFERENCES connector.schema_snapshot(schema_snapshot_id),
    mapping_json jsonb NOT NULL,
    mapping_hash varchar(64) NOT NULL,
    status varchar(20) NOT NULL DEFAULT 'DRAFT'
        CHECK (status IN ('DRAFT','PUBLISHED','RETIRED')),
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    CONSTRAINT uq_mapping_dataset_version UNIQUE (dataset_id, version)
);
ALTER TABLE connector.dataset
    ADD CONSTRAINT fk_dataset_active_mapping
    FOREIGN KEY (active_mapping_version_id)
    REFERENCES connector.mapping_version(mapping_version_id);

CREATE TABLE connector.schedule (
    schedule_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES platform.workspace(workspace_id),
    dataset_id uuid NOT NULL UNIQUE REFERENCES connector.dataset(dataset_id),
    timing_json jsonb NOT NULL,
    timezone varchar(100) NOT NULL,
    start_date date,
    end_date date,
    status varchar(20) NOT NULL DEFAULT 'PENDING_SYNC'
        CHECK (status IN ('PENDING_SYNC','ACTIVE','PAUSED','ERROR','DISABLED')),
    revision integer NOT NULL DEFAULT 1 CHECK (revision > 0),
    aws_schedule_name varchar(64) UNIQUE,
    aws_schedule_arn text,
    last_error_code varchar(100),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (end_date IS NULL OR start_date IS NULL OR end_date >= start_date)
);
CREATE INDEX ix_schedule_workspace_status ON connector.schedule(workspace_id, status);

CREATE TABLE connector.operation (
    operation_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES platform.workspace(workspace_id),
    connection_id uuid NOT NULL REFERENCES connector.connection(connection_id),
    dataset_id uuid REFERENCES connector.dataset(dataset_id),
    operation_type varchar(32) NOT NULL
        CHECK (operation_type IN ('CONNECTION_TEST','SCHEMA_DISCOVERY','DATASET_SYNC')),
    trigger_type varchar(20) NOT NULL DEFAULT 'MANUAL'
        CHECK (trigger_type IN ('MANUAL','SCHEDULE')),
    idempotency_key varchar(255) NOT NULL,
    status varchar(20) NOT NULL DEFAULT 'QUEUED'
        CHECK (status IN (
            'QUEUED','DISPATCHED','RUNNING','SUCCEEDED','FAILED','SKIPPED','CANCELLED'
        )),
    started_at timestamptz,
    completed_at timestamptz,
    error_code varchar(100),
    error_message text,
    result_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_operation_idempotency UNIQUE (workspace_id, idempotency_key)
);
CREATE INDEX ix_operation_workspace_created
    ON connector.operation(workspace_id, created_at DESC);

CREATE TABLE connector.schedule_occurrence (
    occurrence_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES platform.workspace(workspace_id),
    schedule_id uuid NOT NULL REFERENCES connector.schedule(schedule_id),
    occurrence_key varchar(255) NOT NULL,
    scheduled_at timestamptz NOT NULL,
    status varchar(32) NOT NULL
        CHECK (status IN ('RECEIVED','QUEUED','SKIPPED_ACTIVE_RUN','FAILED')),
    operation_id uuid REFERENCES connector.operation(operation_id),
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_schedule_occurrence_key UNIQUE (schedule_id, occurrence_key)
);
CREATE INDEX ix_schedule_occurrence_time
    ON connector.schedule_occurrence(schedule_id, scheduled_at DESC);

CREATE TABLE connector.sync_run (
    sync_run_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    operation_id uuid NOT NULL UNIQUE REFERENCES connector.operation(operation_id),
    workspace_id uuid NOT NULL REFERENCES platform.workspace(workspace_id),
    dataset_id uuid NOT NULL REFERENCES connector.dataset(dataset_id),
    status varchar(20) NOT NULL DEFAULT 'QUEUED'
        CHECK (status IN (
            'QUEUED','EXTRACTING','PUBLISHING','INGESTING','STITCHING',
            'SUCCEEDED','FAILED','SKIPPED','CANCELLED'
        )),
    landing_batch_id uuid NOT NULL UNIQUE REFERENCES control.source_landing_batch(batch_id),
    ingestion_batch_id uuid NOT NULL UNIQUE REFERENCES control.ingestion_batch(batch_id),
    stitching_job_id uuid NOT NULL UNIQUE REFERENCES control.stitching_job(job_id),
    mapping_version_id uuid NOT NULL REFERENCES connector.mapping_version(mapping_version_id),
    checkpoint_from jsonb,
    checkpoint_to jsonb,
    manifest_bucket text,
    manifest_key text,
    records_read bigint NOT NULL DEFAULT 0 CHECK (records_read >= 0),
    records_loaded bigint NOT NULL DEFAULT 0 CHECK (records_loaded >= 0),
    records_rejected bigint NOT NULL DEFAULT 0 CHECK (records_rejected >= 0),
    bytes_written bigint NOT NULL DEFAULT 0 CHECK (bytes_written >= 0),
    started_at timestamptz,
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX uq_sync_run_active_dataset
    ON connector.sync_run(dataset_id)
    WHERE status IN ('QUEUED','EXTRACTING','PUBLISHING','INGESTING','STITCHING');
CREATE INDEX ix_sync_run_workspace_created
    ON connector.sync_run(workspace_id, created_at DESC);

CREATE TABLE connector.sync_partition (
    partition_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    sync_run_id uuid NOT NULL REFERENCES connector.sync_run(sync_run_id),
    partition_no integer NOT NULL CHECK (partition_no > 0),
    boundary_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    status varchar(20) NOT NULL DEFAULT 'QUEUED'
        CHECK (status IN ('QUEUED','RUNNING','SUCCEEDED','FAILED')),
    attempt integer NOT NULL DEFAULT 1 CHECK (attempt > 0),
    s3_bucket text,
    s3_key text,
    checksum_sha256 varchar(64),
    row_count bigint NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    byte_count bigint NOT NULL DEFAULT 0 CHECK (byte_count >= 0),
    error_code varchar(100),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_sync_partition_number UNIQUE (sync_run_id, partition_no)
);
CREATE INDEX ix_sync_partition_status ON connector.sync_partition(status, updated_at);

CREATE TABLE connector.checkpoint (
    dataset_id uuid PRIMARY KEY REFERENCES connector.dataset(dataset_id),
    cursor_json jsonb NOT NULL,
    committed_sync_run_id uuid NOT NULL REFERENCES connector.sync_run(sync_run_id),
    version integer NOT NULL DEFAULT 1 CHECK (version > 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE connector.rejected_record (
    rejected_record_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    sync_run_id uuid NOT NULL REFERENCES connector.sync_run(sync_run_id),
    partition_id uuid REFERENCES connector.sync_partition(partition_id),
    source_record_key_hash varchar(64),
    error_code varchar(100) NOT NULL,
    quarantine_s3_key text,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_rejected_record_run ON connector.rejected_record(sync_run_id);

CREATE TABLE connector.outbox_event (
    outbox_event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES platform.workspace(workspace_id),
    event_type varchar(64) NOT NULL,
    aggregate_id uuid NOT NULL,
    deduplication_key varchar(255) NOT NULL UNIQUE,
    payload_json jsonb NOT NULL,
    status varchar(20) NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','PROCESSING','PROCESSED','FAILED')),
    attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    available_at timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_outbox_pending
    ON connector.outbox_event(status, available_at)
    WHERE status IN ('PENDING','FAILED','PROCESSING');
"""


def upgrade() -> None:
    op.execute(DDL(DDL_STATEMENTS))


def downgrade() -> None:
    op.execute("DROP SCHEMA IF EXISTS connector CASCADE")
    op.execute("DROP SCHEMA IF EXISTS platform CASCADE")
