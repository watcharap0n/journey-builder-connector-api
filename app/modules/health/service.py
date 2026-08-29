import asyncio
from collections.abc import Awaitable

from app.infrastructure.cache.redis import RedisManager
from app.infrastructure.database.session import DatabaseManager
from app.modules.health.schemas import DependencyStatus, ReadinessResponse


class HealthService:
    def __init__(self, database: DatabaseManager, redis: RedisManager) -> None:
        self.database = database
        self.redis = redis

    @staticmethod
    async def _check(operation: Awaitable[object]) -> DependencyStatus:
        try:
            await operation
            return DependencyStatus(status="ok")
        except Exception:
            return DependencyStatus(status="error")

    async def readiness(self) -> ReadinessResponse:
        database_status, redis_status = await asyncio.gather(
            self._check(self.database.ping()),
            self._check(self.redis.ping()),
        )
        status = (
            "ok"
            if database_status.status == "ok" and redis_status.status == "ok"
            else "degraded"
        )
        return ReadinessResponse(
            status=status,
            database=database_status,
            redis=redis_status,
        )
