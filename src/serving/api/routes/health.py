"""
Health Check Endpoints

Provides health and readiness checks for orchestration systems.
"""

from datetime import datetime
from typing import Dict, Any

from fastapi import APIRouter, Response
from pydantic import BaseModel

from src.config import get_settings
from src.database.connection import check_database_health

settings = get_settings()
router = APIRouter()


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    version: str
    environment: str
    timestamp: datetime
    checks: Dict[str, Any]


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """
    Comprehensive health check endpoint.
    
    Checks:
    - Application status
    - Database connectivity
    - Redis connectivity
    """
    checks = {}
    overall_status = "healthy"
    
    # Check database
    try:
        db_health = await check_database_health()
        checks["database"] = db_health
        if db_health.get("status") != "healthy":
            overall_status = "degraded"
    except Exception as e:
        checks["database"] = {"status": "unhealthy", "error": str(e)}
        overall_status = "unhealthy"
    
    # Check Redis
    try:
        from src.serving.cache import get_redis
        redis = get_redis()
        await redis.ping()
        checks["redis"] = {"status": "healthy"}
    except Exception as e:
        checks["redis"] = {"status": "unhealthy", "error": str(e)}
        if overall_status == "healthy":
            overall_status = "degraded"
    
    return HealthResponse(
        status=overall_status,
        version=settings.version,
        environment=settings.app_env,
        timestamp=datetime.utcnow(),
        checks=checks,
    )


@router.get("/health/live")
async def liveness_check() -> Dict[str, str]:
    """
    Kubernetes liveness probe endpoint.
    
    Returns 200 if the application is running.
    """
    return {"status": "alive"}


@router.get("/health/ready")
async def readiness_check(response: Response) -> Dict[str, str]:
    """
    Kubernetes readiness probe endpoint.
    
    Returns 200 if the application is ready to receive traffic.
    """
    try:
        # Check critical dependencies
        db_health = await check_database_health()
        
        if db_health.get("status") != "healthy":
            response.status_code = 503
            return {"status": "not_ready", "reason": "database_unavailable"}
        
        return {"status": "ready"}
    except Exception as e:
        response.status_code = 503
        return {"status": "not_ready", "reason": str(e)}
