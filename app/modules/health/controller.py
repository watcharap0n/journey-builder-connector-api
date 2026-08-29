from fastapi import APIRouter, Response, status

from app.infrastructure.cache.redis import redis_manager
from app.infrastructure.database.session import database_manager
from app.modules.health.schemas import LivenessResponse, ReadinessResponse
from app.modules.health.service import HealthService

router = APIRouter()


@router.get("/live", response_model=LivenessResponse)
async def liveness() -> LivenessResponse:
    return LivenessResponse()


@router.get("/ready", response_model=ReadinessResponse)
async def readiness(response: Response) -> ReadinessResponse:
    result = await HealthService(database_manager, redis_manager).readiness()
    if result.status != "ok":
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return result
