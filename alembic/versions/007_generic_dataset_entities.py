"""Allow the generic dataset types TRANSACTION and EVENT besides CUSTOMER.

Revision ID: 007_generic_dataset_entities
Revises: 006_azure_sql_connector
"""

from alembic import op

revision = "007_generic_dataset_entities"
down_revision = "006_azure_sql_connector"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE connector.dataset DROP CONSTRAINT IF EXISTS dataset_target_entity_check"
    )
    op.execute("""
        ALTER TABLE connector.dataset
        ADD CONSTRAINT dataset_target_entity_check
        CHECK (target_entity IN ('CUSTOMER', 'TRANSACTION', 'EVENT'))
    """)


def downgrade() -> None:
    # Refuses (constraint violation) while any TRANSACTION/EVENT dataset exists.
    op.execute(
        "ALTER TABLE connector.dataset DROP CONSTRAINT IF EXISTS dataset_target_entity_check"
    )
    op.execute("""
        ALTER TABLE connector.dataset
        ADD CONSTRAINT dataset_target_entity_check CHECK (target_entity = 'CUSTOMER')
    """)
