"""Retail dataset types: bill lines, product, store, campaign, consent, points, coupons.

Revision ID: 009_retail_dataset_types
Revises: 008_intake_v2_multi_and_file_upload
"""

from alembic import op

revision = "009_retail_dataset_types"
down_revision = "008_intake_v2_multi_and_file_upload"
branch_labels = None
depends_on = None

TYPES = ("'CUSTOMER', 'TRANSACTION', 'EVENT', 'ASSET', 'RELATIONSHIP', 'TRANSACTION_ITEM', "
         "'PRODUCT', 'STORE', 'CAMPAIGN', 'CONSENT', 'POINT_LEDGER', 'POINT_BALANCE', 'COUPON', "
         "'COUPON_WALLET', 'MULTI'")
OLD_TYPES = "'CUSTOMER', 'TRANSACTION', 'EVENT', 'ASSET', 'RELATIONSHIP', 'MULTI'"


def _check(types: str) -> None:
    op.execute(
        "ALTER TABLE connector.dataset DROP CONSTRAINT IF EXISTS dataset_target_entity_check"
    )
    op.execute(f"""
        ALTER TABLE connector.dataset
        ADD CONSTRAINT dataset_target_entity_check CHECK (target_entity IN ({types}))
    """)


def upgrade() -> None:
    _check(TYPES)


def downgrade() -> None:
    # Refuses while a dataset of a new type exists (the constraint check fails).
    _check(OLD_TYPES)
