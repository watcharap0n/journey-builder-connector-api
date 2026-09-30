"""Persist literal CDP app ownership on connector workspaces.

Revision ID: 005_workspace_app_id
Revises: 004_connector_operation_leases
"""

import sqlalchemy as sa

from alembic import op

revision = "005_workspace_app_id"
down_revision = "004_connector_operation_leases"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Existing workspaces remain unmapped until explicitly reconciled; do not
    # infer their app from slug, source code or historical connector rows.
    op.add_column("workspace", sa.Column("app_id", sa.Text(), nullable=True), schema="platform")
    op.create_check_constraint(
        op.f("ck_workspace_workspace_app_id_decimal"), "workspace",
        "app_id IS NULL OR (app_id ~ '^[0-9]+$' AND length(app_id) <= 97)", schema="platform",
    )


def downgrade() -> None:
    installed = op.get_bind().execute(sa.text("""
        SELECT to_regprocedure('control.connector_batch_app_scope(uuid)') IS NOT NULL
           OR to_regprocedure('control.bind_connector_batch_app_id(uuid,uuid,text)') IS NOT NULL
    """)).scalar_one()
    if installed:
        raise RuntimeError(
            "Remove connector app lineage SQL migration before downgrading workspace app_id"
        )
    op.drop_constraint(
        op.f("ck_workspace_workspace_app_id_decimal"),
        "workspace", schema="platform", type_="check"
    )
    op.drop_column("workspace", "app_id", schema="platform")
