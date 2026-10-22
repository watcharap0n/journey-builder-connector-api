"""Intake v2: ASSET / RELATIONSHIP / MULTI dataset types and the file_upload connector.

Revision ID: 008_intake_v2_multi_and_file_upload
Revises: 007_generic_dataset_entities
"""

from alembic import op

revision = "008_intake_v2_multi_and_file_upload"
down_revision = "007_generic_dataset_entities"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE connector.dataset DROP CONSTRAINT IF EXISTS dataset_target_entity_check"
    )
    op.execute("""
        ALTER TABLE connector.dataset
        ADD CONSTRAINT dataset_target_entity_check CHECK (target_entity IN
            ('CUSTOMER', 'TRANSACTION', 'EVENT', 'ASSET', 'RELATIONSHIP', 'MULTI'))
    """)
    op.execute("""
        INSERT INTO connector.definition (key, family, adapter_key, capabilities)
        VALUES ('file_upload', 'file', 'builtin.upload.v1',
                jsonb_build_object('snapshot', true, 'incremental', false, 'cdc', false))
        ON CONFLICT (key) DO UPDATE
        SET family = EXCLUDED.family,
            adapter_key = EXCLUDED.adapter_key,
            capabilities = EXCLUDED.capabilities,
            is_active = true
    """)


def downgrade() -> None:
    # Both statements refuse (constraint/FK) while upload connections or v2 datasets exist.
    op.execute("""
        DELETE FROM connector.definition AS d
        WHERE d.key = 'file_upload'
          AND NOT EXISTS (
              SELECT 1 FROM connector.connection AS c WHERE c.definition_key = d.key
          )
    """)
    op.execute(
        "ALTER TABLE connector.dataset DROP CONSTRAINT IF EXISTS dataset_target_entity_check"
    )
    op.execute("""
        ALTER TABLE connector.dataset
        ADD CONSTRAINT dataset_target_entity_check
        CHECK (target_entity IN ('CUSTOMER', 'TRANSACTION', 'EVENT'))
    """)
