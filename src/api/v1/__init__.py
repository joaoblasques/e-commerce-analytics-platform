"""
API v1 module.

This module contains all v1 API endpoints and routers.
"""

from fastapi import APIRouter

from .endpoints import analytics, auth, customers, fraud, health

# Create the main API router for v1
api_router = APIRouter()

# Include endpoint routers
api_router.include_router(health.router, prefix="/health", tags=["health"])

api_router.include_router(auth.router, prefix="/auth", tags=["authentication"])

api_router.include_router(customers.router, prefix="/customers", tags=["customers"])

api_router.include_router(analytics.router, prefix="/analytics", tags=["analytics"])

api_router.include_router(fraud.router, prefix="/fraud", tags=["fraud"])


__all__ = ["api_router"]
