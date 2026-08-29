import json
from typing import Any

from redis.asyncio import Redis

from app.core.config import get_settings


class RedisManager:
    def __init__(self) -> None:
        self._client: Redis | None = None

    def configure(self, redis_url: str) -> None:
        if self._client is not None:
            raise RuntimeError("RedisManager is already configured")
        self._client = Redis.from_url(redis_url, encoding="utf-8", decode_responses=True)

    @property
    def client(self) -> Redis:
        if self._client is None:
            raise RuntimeError("RedisManager is not configured")
        return self._client

    async def ping(self) -> None:
        await self.client.ping()

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
        self._client = None


class RedisCache:
    def __init__(self, client: Redis, prefix: str, default_ttl_seconds: int) -> None:
        self.client = client
        self.prefix = prefix.rstrip(":")
        self.default_ttl_seconds = default_ttl_seconds

    def _key(self, key: str) -> str:
        return f"{self.prefix}:{key}"

    async def get_json(self, key: str) -> Any | None:
        value = await self.client.get(self._key(key))
        return None if value is None else json.loads(value)

    async def set_json(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        ttl = ttl_seconds or self.default_ttl_seconds
        await self.client.set(self._key(key), json.dumps(value), ex=ttl)

    async def delete(self, key: str) -> None:
        await self.client.delete(self._key(key))


redis_manager = RedisManager()


def get_cache() -> RedisCache:
    settings = get_settings()
    return RedisCache(
        client=redis_manager.client,
        prefix=settings.cache_prefix,
        default_ttl_seconds=settings.cache_default_ttl_seconds,
    )
