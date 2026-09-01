import pytest

from app.core.config import Settings
from app.infrastructure.secrets import aws as database_secrets
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


def test_local_host_process_maps_docker_host_to_forwarded_loopback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(database_secrets, "_is_container_runtime", lambda: False)
    settings = Settings(
        _env_file=None,
        app_env="local",
        database_url=(
            "postgresql://user:password@host.docker.internal:55432/postgres"
        ),
    )

    url = resolve_database_url_sync(settings)

    assert "@127.0.0.1:55432/postgres" in url


def test_local_container_keeps_docker_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(database_secrets, "_is_container_runtime", lambda: True)
    settings = Settings(
        _env_file=None,
        app_env="local",
        database_url=(
            "postgresql://user:password@host.docker.internal:55432/postgres"
        ),
    )

    url = resolve_database_url_sync(settings)

    assert "@host.docker.internal:55432/postgres" in url


def test_local_secret_manager_host_maps_to_forwarded_loopback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _SecretsManagerClient:
        def get_secret_value(self, *, SecretId: str) -> dict[str, str]:
            assert SecretId == "local-forwarded-database"
            return {
                "SecretString": (
                    '{"host":"host.docker.internal","port":55432,'
                    '"username":"postgres","password":"p@ss?word",'
                    '"dbname":"postgres"}'
                )
            }

    monkeypatch.setattr(database_secrets, "_is_container_runtime", lambda: False)
    monkeypatch.setattr(
        database_secrets.boto3,
        "client",
        lambda *_args, **_kwargs: _SecretsManagerClient(),
    )
    settings = Settings(
        _env_file=None,
        app_env="local",
        database_url=None,
        database_secret_id="local-forwarded-database",
    )

    url = resolve_database_url_sync(settings)

    assert url == (
        "postgresql+asyncpg://postgres:p%40ss%3Fword@127.0.0.1:55432/postgres"
    )
