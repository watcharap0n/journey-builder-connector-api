"""Add the manual incremental standardization control plane.

Revision ID: 002_standardization_control_plane
Revises: 001_connector_control_plane
Create Date: 2026-09-01
"""

from sqlalchemy import DDL

from alembic import op

revision = "002_standardization_control_plane"
down_revision = "001_connector_control_plane"
branch_labels = None
depends_on = None


DDL_STATEMENTS = r"""
ALTER TABLE control.standardization_dataset
    ADD COLUMN IF NOT EXISTS workspace_id uuid
        REFERENCES platform.workspace(workspace_id);
CREATE INDEX IF NOT EXISTS ix_standardization_dataset_workspace_active
    ON control.standardization_dataset(workspace_id, is_active, dataset_id);

ALTER TABLE control.standardization_run
    ADD COLUMN IF NOT EXISTS workspace_id uuid
        REFERENCES platform.workspace(workspace_id),
    ADD COLUMN IF NOT EXISTS trigger_type text NOT NULL DEFAULT 'MANUAL',
    ADD COLUMN IF NOT EXISTS discovered_index_count integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS queued_index_count integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS skipped_index_count integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS new_record_count bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS duplicate_record_count bigint NOT NULL DEFAULT 0;

DO $block$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'control.standardization_run'::regclass
          AND conname = 'ck_standardization_run_trigger_type'
    ) THEN
        ALTER TABLE control.standardization_run
            ADD CONSTRAINT ck_standardization_run_trigger_type
            CHECK (trigger_type = 'MANUAL');
    END IF;
END
$block$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_standardization_run_active_dataset
    ON control.standardization_run(dataset_id)
    WHERE run_status IN ('PENDING', 'RUNNING');
CREATE INDEX IF NOT EXISTS ix_standardization_run_workspace_created
    ON control.standardization_run(workspace_id, created_at DESC);

ALTER TABLE control.standardization_checkpoint
    ADD COLUMN IF NOT EXISTS stream_kind text NOT NULL DEFAULT 'baseline',
    ADD COLUMN IF NOT EXISTS start_cursor jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS cutoff_cursor jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS attempt integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS new_record_count bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS duplicate_record_count bigint NOT NULL DEFAULT 0;

DO $block$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'control.standardization_checkpoint'::regclass
          AND conname = 'ck_standardization_checkpoint_attempt'
    ) THEN
        ALTER TABLE control.standardization_checkpoint
            ADD CONSTRAINT ck_standardization_checkpoint_attempt CHECK (attempt >= 0);
    END IF;
END
$block$;

DO $block$
DECLARE
    current_columns text[];
BEGIN
    SELECT array_agg(att.attname ORDER BY ord.n)
    INTO current_columns
    FROM pg_constraint con
    CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS ord(attnum, n)
    JOIN pg_attribute att
      ON att.attrelid = con.conrelid AND att.attnum = ord.attnum
    WHERE con.conrelid = 'control.standardization_checkpoint'::regclass
      AND con.contype = 'p';

    IF current_columns IS DISTINCT FROM
       ARRAY['run_id', 'source_partition_key', 'stream_kind']::text[] THEN
        ALTER TABLE control.standardization_checkpoint
            DROP CONSTRAINT IF EXISTS standardization_checkpoint_pkey;
        ALTER TABLE control.standardization_checkpoint
            ADD CONSTRAINT standardization_checkpoint_pkey
            PRIMARY KEY (run_id, source_partition_key, stream_kind);
    END IF;
END
$block$;

DO $block$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'control.standardization_checkpoint'::regclass
          AND conname = 'ck_standardization_checkpoint_stream_kind'
    ) THEN
        ALTER TABLE control.standardization_checkpoint
            ADD CONSTRAINT ck_standardization_checkpoint_stream_kind
            CHECK (stream_kind IN ('baseline', 'versions'));
    END IF;
END
$block$;

CREATE TABLE IF NOT EXISTS control.standardization_partition_state (
    dataset_id bigint NOT NULL
        REFERENCES control.standardization_dataset(dataset_id),
    source_partition_key text NOT NULL,
    stream_kind text NOT NULL
        CHECK (stream_kind IN ('baseline', 'versions')),
    committed_cursor jsonb NOT NULL DEFAULT '{}'::jsonb,
    committed_run_id uuid REFERENCES control.standardization_run(run_id),
    is_complete boolean NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset_id, source_partition_key, stream_kind),
    CHECK (btrim(source_partition_key) <> ''),
    CHECK (jsonb_typeof(committed_cursor) = 'object')
);
CREATE INDEX IF NOT EXISTS ix_standardization_partition_state_dataset
    ON control.standardization_partition_state(dataset_id, stream_kind, source_partition_key);

CREATE INDEX IF NOT EXISTS ix_standardization_checkpoint_reconcile
    ON control.standardization_checkpoint(checkpoint_status, updated_at, run_id)
    WHERE checkpoint_status IN ('RUNNING','FAILED');

CREATE TABLE IF NOT EXISTS control.standardization_outbox_event (
    outbox_event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES platform.workspace(workspace_id),
    run_id uuid NOT NULL REFERENCES control.standardization_run(run_id),
    source_partition_key text NOT NULL,
    stream_kind text NOT NULL CHECK (stream_kind IN ('baseline', 'versions')),
    event_type text NOT NULL CHECK (event_type = 'standardization.partition.dispatch'),
    deduplication_key text NOT NULL UNIQUE,
    payload_json jsonb NOT NULL,
    status text NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','PROCESSING','PROCESSED','FAILED')),
    attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    available_at timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (jsonb_typeof(payload_json) = 'object')
);
CREATE INDEX IF NOT EXISTS ix_standardization_outbox_pending
    ON control.standardization_outbox_event(status, available_at)
    WHERE status IN ('PENDING','FAILED','PROCESSING');
"""


def upgrade() -> None:
    op.execute(DDL(DDL_STATEMENTS))


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS control.standardization_outbox_event")
    op.execute("DROP TABLE IF EXISTS control.standardization_partition_state")
    op.execute("DROP INDEX IF EXISTS control.uq_standardization_run_active_dataset")
