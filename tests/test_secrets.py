from app.infrastructure.secrets.aws import build_database_url, parse_database_secret


def test_database_secret_uses_postgres_as_default_dbname() -> None:
    secret = parse_database_secret(
        '{"host":"db.internal","username":"app","password":"p@ss/word"}'
    )

    url = build_database_url(secret)

    assert secret.dbname == "postgres"
    assert url == "postgresql+asyncpg://app:p%40ss%2Fword@db.internal:5432/postgres"
