"""Stock dataset type: stock on hand per product and location (STOCK_BALANCE).

Revision ID: 010_stock_dataset_type
Revises: 009_retail_dataset_types
"""

from alembic import op

revision = "010_stock_dataset_type"
down_revision = "009_retail_dataset_types"
branch_labels = None
depends_on = None

TYPES = ("'CUSTOMER', 'TRANSACTION', 'EVENT', 'ASSET', 'RELATIONSHIP', 'TRANSACTION_ITEM', "
         "'PRODUCT', 'STORE', 'CAMPAIGN', 'CONSENT', 'POINT_LEDGER', 'POINT_BALANCE', 'COUPON', "
         "'COUPON_WALLET', 'STOCK_BALANCE', 'MULTI'")
OLD_TYPES = ("'CUSTOMER', 'TRANSACTION', 'EVENT', 'ASSET', 'RELATIONSHIP', "
             "'TRANSACTION_ITEM', 'PRODUCT', 'STORE', 'CAMPAIGN', 'CONSENT', 'POINT_LEDGER', "
             "'POINT_BALANCE', 'COUPON', 'COUPON_WALLET', 'MULTI'")


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
    # Refuses while a STOCK_BALANCE dataset exists (the constraint check fails).
    _check(OLD_TYPES)
