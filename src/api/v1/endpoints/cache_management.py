"""
Cache management endpoints for the E-Commerce Analytics Platform API.

This module provides endpoints for managing the Redis cache, including
cache statistics, invalidation, and performance monitoring.
"""

from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from ...auth.dependencies import get_current_active_user, require_permission
from ...auth.models import Permission, User
from ...dependencies import get_database_session
from ...middleware.compression import create_optimized_response
from ...services.cache import (
    cache_clear_pattern,
    cache_invalidate_analytics,
    cache_invalidate_customers,
    cache_invalidate_fraud,
    cache_invalidate_realtime,
    get_cache_service,
)

router = APIRouter()


@router.get("/stats", summary="Get cache statistics")
async def get_cache_stats(
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_ANALYTICS)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    Get Redis cache statistics and performance metrics.

    Returns detailed information about cache usage, hit rates,
    memory consumption, and connection status.
    """
    cache_service = get_cache_service()
    stats = cache_service.get_cache_stats()

    # Add additional performance insights
    if stats.get("connected", False):
        stats["performance_insights"] = {
            "hit_rate_category": (
                "excellent"
                if stats.get("hit_rate", 0) >= 80
                else "good"
                if stats.get("hit_rate", 0) >= 60
                else "fair"
                if stats.get("hit_rate", 0) >= 40
                else "poor"
            ),
            "optimization_suggestions": _get_cache_optimization_suggestions(stats),
        }

    return create_optimized_response(stats)


@router.post("/invalidate/analytics", summary="Invalidate analytics cache")
async def invalidate_analytics_cache(
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.WRITE_ANALYTICS)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    Invalidate all analytics-related cache entries.

    This forces fresh data to be loaded for all analytics endpoints
    on the next request.
    """
    cleared_count = cache_invalidate_analytics()

    return create_optimized_response(
        {
            "message": f"Analytics cache invalidated successfully",
            "cleared_entries": cleared_count,
            "affected_endpoints": [
                "revenue analytics",
                "customer segmentation",
                "product performance",
                "cohort analysis",
                "funnel analysis",
            ],
        }
    )


@router.post("/invalidate/customers", summary="Invalidate customer cache")
async def invalidate_customers_cache(
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.WRITE_CUSTOMERS)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    Invalidate all customer-related cache entries.

    This forces fresh data to be loaded for all customer endpoints
    on the next request.
    """
    cleared_count = cache_invalidate_customers()

    return create_optimized_response(
        {
            "message": f"Customer cache invalidated successfully",
            "cleared_entries": cleared_count,
            "affected_endpoints": [
                "customer list",
                "customer details",
                "customer recommendations",
                "customer value predictions",
            ],
        }
    )


@router.post("/invalidate/fraud", summary="Invalidate fraud detection cache")
async def invalidate_fraud_cache(
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.WRITE_FRAUD)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    Invalidate all fraud detection cache entries.

    This forces fresh data to be loaded for all fraud endpoints
    on the next request.
    """
    cleared_count = cache_invalidate_fraud()

    return create_optimized_response(
        {
            "message": f"Fraud detection cache invalidated successfully",
            "cleared_entries": cleared_count,
            "affected_endpoints": [
                "fraud alerts",
                "fraud dashboard",
                "fraud model performance",
                "investigation cases",
                "risk assessments",
            ],
        }
    )


@router.post("/invalidate/realtime", summary="Invalidate real-time cache")
async def invalidate_realtime_cache(
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_ANALYTICS)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    Invalidate all real-time metrics cache entries.

    This forces fresh data to be loaded for all real-time endpoints
    on the next request.
    """
    cleared_count = cache_invalidate_realtime()

    return create_optimized_response(
        {
            "message": f"Real-time metrics cache invalidated successfully",
            "cleared_entries": cleared_count,
            "affected_endpoints": [
                "system health",
                "stream metrics",
                "business metrics",
                "performance benchmarks",
            ],
        }
    )


@router.post("/invalidate/all", summary="Invalidate all cache")
async def invalidate_all_cache(
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.MANAGE_SYSTEM)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    Invalidate all cache entries across all categories.

    This is a comprehensive cache clear that affects all endpoints.
    Use with caution as it will impact performance until caches rebuild.
    """
    analytics_count = cache_invalidate_analytics()
    customers_count = cache_invalidate_customers()
    fraud_count = cache_invalidate_fraud()
    realtime_count = cache_invalidate_realtime()

    total_count = analytics_count + customers_count + fraud_count + realtime_count

    return create_optimized_response(
        {
            "message": "All cache entries invalidated successfully",
            "total_cleared_entries": total_count,
            "breakdown": {
                "analytics": analytics_count,
                "customers": customers_count,
                "fraud": fraud_count,
                "realtime": realtime_count,
            },
            "warning": "Performance may be impacted until caches rebuild",
        }
    )


@router.post("/invalidate/pattern", summary="Invalidate cache by pattern")
async def invalidate_cache_pattern(
    pattern: str,
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.MANAGE_SYSTEM)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    Invalidate cache entries matching a specific pattern.

    Allows for granular cache invalidation using Redis key patterns.
    Supports wildcards (* and ?) for flexible matching.

    Args:
        pattern: Redis key pattern to match (e.g., "analytics:revenue:*")
    """
    # Validate pattern to prevent overly broad invalidation
    if pattern in ["*", "**", "*:*"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pattern too broad. Use specific cache invalidation endpoints instead.",
        )

    cleared_count = cache_clear_pattern(pattern)

    return create_optimized_response(
        {
            "message": f"Cache entries matching pattern '{pattern}' invalidated successfully",
            "pattern": pattern,
            "cleared_entries": cleared_count,
        }
    )


@router.get("/keys", summary="List cache keys")
async def list_cache_keys(
    pattern: str = "*",
    limit: int = 100,
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_ANALYTICS)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    List cache keys matching a pattern.

    Useful for debugging and understanding cache usage patterns.

    Args:
        pattern: Redis key pattern to match (default: "*")
        limit: Maximum number of keys to return (default: 100)
    """
    cache_service = get_cache_service()

    if not cache_service.redis_client:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Redis is not available",
        )

    try:
        keys = cache_service.redis_client.keys(pattern)[:limit]
        key_info = []

        for key in keys:
            try:
                ttl = cache_service.redis_client.ttl(key)
                key_info.append(
                    {
                        "key": key,
                        "ttl": ttl if ttl > 0 else None,
                        "expires": "never" if ttl == -1 else f"{ttl}s",
                    }
                )
            except Exception:
                key_info.append({"key": key, "ttl": None, "expires": "unknown"})

        return create_optimized_response(
            {
                "pattern": pattern,
                "total_found": len(keys),
                "limit": limit,
                "keys": key_info[:limit],
            }
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listing cache keys: {str(e)}",
        )


@router.get("/health", summary="Check cache health")
async def check_cache_health(
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_ANALYTICS)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    Check Redis cache health and connectivity.

    Returns detailed health information including connection status,
    response times, and basic performance metrics.
    """
    cache_service = get_cache_service()

    health_data = {
        "timestamp": "2025-01-23T00:00:00Z",  # Would be actual timestamp
        "redis_connected": False,
        "response_time_ms": None,
        "status": "unhealthy",
    }

    if cache_service.redis_client:
        try:
            import time

            start_time = time.time()

            # Test basic operations
            test_key = "health_check_test"
            cache_service.set(test_key, "test_value", 5)
            result = cache_service.get(test_key)
            cache_service.delete(test_key)

            end_time = time.time()
            response_time = (end_time - start_time) * 1000  # Convert to milliseconds

            if result == "test_value":
                health_data.update(
                    {
                        "redis_connected": True,
                        "response_time_ms": round(response_time, 2),
                        "status": "healthy" if response_time < 100 else "slow",
                        "performance": (
                            "excellent"
                            if response_time < 10
                            else "good"
                            if response_time < 50
                            else "fair"
                            if response_time < 100
                            else "poor"
                        ),
                    }
                )

        except Exception as e:
            health_data.update({"error": str(e), "status": "error"})

    return create_optimized_response(health_data)


def _get_cache_optimization_suggestions(stats: Dict[str, Any]) -> List[str]:
    """
    Generate cache optimization suggestions based on statistics.

    Args:
        stats: Cache statistics

    Returns:
        List of optimization suggestions
    """
    suggestions = []

    hit_rate = stats.get("hit_rate", 0)
    if hit_rate < 40:
        suggestions.append("Consider increasing cache TTL for frequently accessed data")
        suggestions.append("Review caching strategy for endpoints with low hit rates")

    if hit_rate < 20:
        suggestions.append(
            "Cache may not be effectively utilized - review cache keys and patterns"
        )

    connected_clients = stats.get("connected_clients", 0)
    if connected_clients > 100:
        suggestions.append(
            "High number of Redis connections - consider connection pooling"
        )

    # Memory usage suggestions would go here based on used_memory
    # This is a simplified version
    suggestions.append("Monitor memory usage and consider cache eviction policies")

    if not suggestions:
        suggestions.append("Cache performance is optimal")

    return suggestions
