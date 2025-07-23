"""
Health check endpoints for API v1.

This module provides health check and system status endpoints.
"""

import time
from typing import Any, Dict

from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session

from ...config import get_settings
from ...dependencies import (
    get_database_session,
    get_redis_dependency,
    validate_database_connection,
    validate_redis_connection,
)

router = APIRouter()


@router.get("/", summary="Basic health check")
async def health_check() -> Dict[str, Any]:
    """
    Basic health check endpoint.

    Returns:
        Dict containing basic health status
    """
    return {"status": "healthy", "timestamp": time.time(), "version": "1.0.0"}


@router.get("/detailed", summary="Detailed health check")
async def detailed_health_check(
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    Detailed health check with dependency validation.

    Args:
        db: Database session dependency

    Returns:
        Dict containing detailed health status including database connectivity
    """
    health_status = {
        "status": "healthy",
        "timestamp": time.time(),
        "version": "1.0.0",
        "services": {},
    }

    # Check database
    try:
        await validate_database_connection()
        health_status["services"]["database"] = {
            "status": "healthy",
            "message": "Database connection successful",
        }
    except Exception as e:
        health_status["status"] = "degraded"
        health_status["services"]["database"] = {
            "status": "unhealthy",
            "message": f"Database connection failed: {str(e)}",
        }

    # Check Redis
    try:
        await validate_redis_connection()
        health_status["services"]["redis"] = {
            "status": "healthy",
            "message": "Redis connection successful",
        }
    except Exception as e:
        health_status["status"] = "degraded"
        health_status["services"]["redis"] = {
            "status": "unhealthy",
            "message": f"Redis connection failed: {str(e)}",
        }

    # Add environment info
    settings = get_settings()
    health_status["environment"] = {
        "mode": settings.environment,
        "debug": settings.debug,
        "version": "1.0.0",
    }

    return health_status


@router.get("/readiness", summary="Readiness probe")
async def readiness_check() -> Dict[str, str]:
    """
    Kubernetes readiness probe endpoint.

    Returns:
        Dict containing readiness status
    """
    # Check critical dependencies
    try:
        await validate_database_connection()
        return {"status": "ready"}
    except Exception:
        return {"status": "not_ready"}


@router.get("/liveness", summary="Liveness probe")
async def liveness_check() -> Dict[str, str]:
    """
    Kubernetes liveness probe endpoint.

    Returns:
        Dict containing liveness status
    """
    return {"status": "alive"}
