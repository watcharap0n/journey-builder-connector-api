import asyncio
import base64
import json
import os
from collections.abc import Mapping
from pathlib import Path

import boto3
from pydantic import BaseModel, ConfigDict, SecretStr
from sqlalchemy import URL, make_url

from app.core.config import Settings


class DatabaseSecret(BaseModel):
    model_config = ConfigDict(extra="ignore")

    host: str
    port: int = 5432
    username: str
    password: SecretStr
    dbname: str = "postgres"


def _as_asyncpg_url(url: str) -> str:
    parsed = make_url(url)
    if parsed.drivername in {"postgres", "postgresql"}:
        parsed = parsed.set(drivername="postgresql+asyncpg")
    return parsed.render_as_string(hide_password=False)


def build_database_url(secret: DatabaseSecret) -> str:
    return URL.create(
        drivername="postgresql+asyncpg",
        username=secret.username,
        password=secret.password.get_secret_value(),
        host=secret.host,
        port=secret.port,
        database=secret.dbname,
    ).render_as_string(hide_password=False)


def _is_container_runtime() -> bool:
    return Path("/.dockerenv").exists() or bool(
        os.environ.get("ECS_CONTAINER_METADATA_URI_V4")
        or os.environ.get("ECS_CONTAINER_METADATA_URI")
    )


def _normalize_runtime_host(url: str, settings: Settings) -> str:
    parsed = make_url(url)
    if (
        settings.app_env.lower() == "local"
        and parsed.host == "host.docker.internal"
        and not _is_container_runtime()
    ):
        parsed = parsed.set(host="127.0.0.1")
    return parsed.render_as_string(hide_password=False)


def parse_database_secret(secret_value: str) -> DatabaseSecret:
    try:
        payload = json.loads(secret_value)
    except json.JSONDecodeError as exc:
        raise RuntimeError("AWS database secret is not valid JSON") from exc
    return DatabaseSecret.model_validate(payload)


def _read_secret_value(response: Mapping[str, object]) -> str:
    secret_string = response.get("SecretString")
    if isinstance(secret_string, str):
        return secret_string

    secret_binary = response.get("SecretBinary")
    if isinstance(secret_binary, bytes):
        return base64.b64decode(secret_binary).decode("utf-8")

    raise RuntimeError("AWS Secrets Manager response contained no secret value")


def resolve_database_url_sync(settings: Settings) -> str:
    if settings.database_url:
        return _normalize_runtime_host(_as_asyncpg_url(settings.database_url), settings)

    if settings.database_secret_id:
        client = boto3.client("secretsmanager", region_name=settings.aws_region)
        response = client.get_secret_value(SecretId=settings.database_secret_id)
        url = build_database_url(parse_database_secret(_read_secret_value(response)))
        return _normalize_runtime_host(url, settings)

    raise RuntimeError(
        "Database configuration is missing: set DATABASE_URL or DATABASE_SECRET_ID/SECRET_ID"
    )


async def resolve_database_url(settings: Settings) -> str:
    return await asyncio.to_thread(resolve_database_url_sync, settings)
