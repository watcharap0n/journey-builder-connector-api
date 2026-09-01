"""Add connector dataset preview metadata and operation support.

Revision ID: 003_connector_dataset_preview
Revises: 002_standardization_control_plane
Create Date: 2026-09-01
"""

from alembic import op

revision = "003_connector_dataset_preview"
down_revision = "002_standardization_control_plane"
branch_labels = None
depends_on = None


UPGRADE_DDL = r"""
ALTER TABLE connector.dataset
    ADD COLUMN IF NOT EXISTS row_count_estimate bigint,
    ADD COLUMN IF NOT EXISTS row_count_estimated_at timestamptz;

ALTER TABLE connector.operation
    ADD COLUMN IF NOT EXISTS request_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS result_expires_at timestamptz;

-- Revision 001 created this check inline, so PostgreSQL named it
-- operation_operation_type_check. Also drop the ORM convention/name variants
-- to support databases created through metadata rather than the original DDL.
ALTER TABLE connector.operation
    DROP CONSTRAINT IF EXISTS operation_operation_type_check,
    DROP CONSTRAINT IF EXISTS ck_operation_operation_type,
    DROP CONSTRAINT IF EXISTS operation_type;
ALTER TABLE connector.operation
    ADD CONSTRAINT ck_operation_operation_type CHECK (
        operation_type IN (
            'CONNECTION_TEST',
            'SCHEMA_DISCOVERY',
            'DATASET_PREVIEW',
            'DATASET_SYNC'
        )
    );

CREATE INDEX IF NOT EXISTS ix_operation_preview_expiry
    ON connector.operation(result_expires_at)
    WHERE operation_type = 'DATASET_PREVIEW' AND result_expires_at IS NOT NULL;
"""


DOWNGRADE_DDL = r"""
DELETE FROM connector.operation WHERE operation_type = 'DATASET_PREVIEW';

DROP INDEX IF EXISTS connector.ix_operation_preview_expiry;

ALTER TABLE connector.operation
    DROP CONSTRAINT IF EXISTS ck_operation_operation_type,
    DROP CONSTRAINT IF EXISTS operation_type;
ALTER TABLE connector.operation
    ADD CONSTRAINT operation_operation_type_check CHECK (
        operation_type IN ('CONNECTION_TEST', 'SCHEMA_DISCOVERY', 'DATASET_SYNC')
    );

ALTER TABLE connector.operation
    DROP COLUMN IF EXISTS result_expires_at,
    DROP COLUMN IF EXISTS request_json;

ALTER TABLE connector.dataset
    DROP COLUMN IF EXISTS row_count_estimated_at,
    DROP COLUMN IF EXISTS row_count_estimate;
"""
def _execute_sql_block(sql_block: str) -> None:
    for statement in (chunk.strip() for chunk in sql_block.split(";") if chunk.strip()):
        op.execute(statement)


def upgrade() -> None:
    _execute_sql_block(UPGRADE_DDL)


def downgrade() -> None:
    _execute_sql_block(DOWNGRADE_DDL)
