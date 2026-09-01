import asyncio
from logging.config import fileConfig

from sqlalchemy import Connection, pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context
from app.core.config import get_settings
from app.infrastructure.database.base import Base
from app.infrastructure.secrets.aws import resolve_database_url_sync
from app.modules.connectors import model as connector_model  # noqa: F401

# Import feature model modules here so Alembic can discover their metadata.
# Example: from app.modules.journeys import model as journey_model  # noqa: F401

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

settings = get_settings()
if context.is_offline_mode():
    database_url = (
        settings.database_url
        or "postgresql+asyncpg://offline:offline@localhost/postgres"
    )
    config.set_main_option(
        "sqlalchemy.url",
        database_url.replace("%", "%%"),
    )
else:
    database_url = resolve_database_url_sync(settings)
    config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
target_metadata = Base.metadata
VERSION_TABLE = "journey_builder_api_alembic_version"
VERSION_TABLE_LENGTH = 128


def ensure_version_table(connection: Connection) -> None:
    connection.exec_driver_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {VERSION_TABLE} (
            version_num VARCHAR({VERSION_TABLE_LENGTH}) NOT NULL,
            CONSTRAINT {VERSION_TABLE}_pk PRIMARY KEY (version_num)
        )
        """
    )
    connection.exec_driver_sql(
        f"ALTER TABLE {VERSION_TABLE} ALTER COLUMN version_num TYPE VARCHAR({VERSION_TABLE_LENGTH})"
    )


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        version_table=VERSION_TABLE,
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    ensure_version_table(connection)
    # ensure_version_table starts SQLAlchemy's implicit transaction before
    # Alembic configures its own transaction. Commit it explicitly so the
    # migration transaction is not rolled back when the async connection closes.
    connection.commit()
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        version_table=VERSION_TABLE,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
