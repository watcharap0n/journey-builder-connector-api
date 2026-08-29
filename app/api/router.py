from fastapi import APIRouter

from app.modules.connectors.controller import router as connector_router
from app.modules.health.controller import router as health_router

api_router = APIRouter()
api_router.include_router(health_router, prefix="/health", tags=["health"])
api_router.include_router(connector_router)
