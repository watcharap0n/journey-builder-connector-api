"""Register Azure SQL Database as a connector source.

Revision ID: 006_azure_sql_connector
Revises: 005_workspace_app_id
"""

from alembic import op

revision = "006_azure_sql_connector"
down_revision = "005_workspace_app_id"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        INSERT INTO connector.definition (key, family, adapter_key, capabilities)
        VALUES ('azure_sql', 'relational', 'builtin.connector.azure_sql.v1',
                jsonb_build_object('snapshot', true, 'incremental', true, 'cdc', false))
        ON CONFLICT (key) DO UPDATE
        SET family = EXCLUDED.family,
            adapter_key = EXCLUDED.adapter_key,
            capabilities = EXCLUDED.capabilities,
            is_active = true
    """)


def downgrade() -> None:
    op.execute("""
        DELETE FROM connector.definition AS d
        WHERE d.key = 'azure_sql'
          AND NOT EXISTS (
              SELECT 1 FROM connector.connection AS c WHERE c.definition_key = d.key
          )
    """)
