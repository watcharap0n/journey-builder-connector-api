import pytest

from app.core.config import Settings
from app.infrastructure.secrets.aws import resolve_database_url_sync


def test_database_configuration_is_required() -> None:
    settings = Settings(_env_file=None, database_url=None, database_secret_id=None)

    with pytest.raises(RuntimeError, match="Database configuration is missing"):
        resolve_database_url_sync(settings)


def test_plain_postgresql_url_is_normalized_for_async_sqlalchemy() -> None:
    settings = Settings(
        _env_file=None,
        database_url="postgresql://user:password@db.internal:5432/postgres",
    )

    url = resolve_database_url_sync(settings)

    assert url.startswith("postgresql+asyncpg://")
