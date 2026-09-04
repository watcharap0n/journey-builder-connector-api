"""Add connector operation execution lease metadata.

Revision ID: 004_connector_operation_leases
Revises: 003_connector_dataset_preview
Create Date: 2026-09-04
"""

import sqlalchemy as sa

from alembic import op

revision = "004_connector_operation_leases"
down_revision = "003_connector_dataset_preview"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "operation",
        sa.Column(
            "execution_attempt",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        schema="connector",
    )
    op.add_column(
        "operation",
        sa.Column("lease_owner", sa.String(length=100), nullable=True),
        schema="connector",
    )
    op.add_column(
        "operation",
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        schema="connector",
    )
    op.execute(
        """
        ALTER TABLE connector.operation
        ADD CONSTRAINT ck_operation_execution_attempt
        CHECK (execution_attempt >= 0) NOT VALID
        """
    )
    op.execute(
        """
        ALTER TABLE connector.operation
        VALIDATE CONSTRAINT ck_operation_execution_attempt
        """
    )


def downgrade() -> None:
    op.drop_constraint(
        op.f("ck_operation_execution_attempt"),
        "operation",
        schema="connector",
        type_="check",
    )
    op.drop_column("operation", "lease_expires_at", schema="connector")
    op.drop_column("operation", "lease_owner", schema="connector")
    op.drop_column("operation", "execution_attempt", schema="connector")
