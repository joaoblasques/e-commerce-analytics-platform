"""
Redis caching service for the E-Commerce Analytics Platform API.

This module provides caching functionality using Redis to improve performance
for frequently accessed analytics data.
"""

import json
import logging
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Union

import redis
from pydantic import BaseModel

from ..config import get_redis_config, get_settings

logger = logging.getLogger(__name__)


class CacheService:
    """Redis-based caching service for analytics data."""

    def __init__(self):
        """Initialize Redis connection."""
        try:
            redis_config = get_redis_config()
            self.redis_client = redis.from_url(**redis_config)
            self.settings = get_settings()

            # Test connection
            self.redis_client.ping()
            logger.info("Redis connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.redis_client = None

    def _serialize_key(self, *args, **kwargs) -> str:
        """Create a serialized cache key from arguments."""
        # Convert args to strings and sort kwargs for consistent keys
        key_parts = [str(arg) for arg in args]
        if kwargs:
            sorted_kwargs = sorted(kwargs.items())
            key_parts.extend([f"{k}:{v}" for k, v in sorted_kwargs])

        return ":".join(key_parts)

    def _serialize_value(self, value: Any) -> str:
        """Serialize value for Redis storage."""
        if isinstance(value, BaseModel):
            return value.model_dump_json()
        elif isinstance(value, dict):
            return json.dumps(value, default=str)
        elif isinstance(value, (list, tuple)):
            return json.dumps(value, default=str)
        else:
            return json.dumps(value, default=str)

    def _deserialize_value(self, value: str) -> Any:
        """Deserialize value from Redis storage."""
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value

    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found
        """
        if not self.redis_client:
            return None

        try:
            cached_value = self.redis_client.get(key)
            if cached_value:
                logger.debug(f"Cache hit for key: {key}")
                return self._deserialize_value(cached_value)
            else:
                logger.debug(f"Cache miss for key: {key}")
                return None
        except Exception as e:
            logger.error(f"Error getting cache value for key {key}: {e}")
            return None

    def set(self, key: str, value: Any, expire_time: Optional[int] = None) -> bool:
        """
        Set value in cache.

        Args:
            key: Cache key
            value: Value to cache
            expire_time: Expiration time in seconds (default from settings)

        Returns:
            True if successfully cached
        """
        if not self.redis_client:
            return False

        try:
            serialized_value = self._serialize_value(value)
            expire_time = expire_time or self.settings.redis_expire_time

            result = self.redis_client.setex(key, expire_time, serialized_value)
            if result:
                logger.debug(f"Cached value for key: {key} (expires in {expire_time}s)")
            return result
        except Exception as e:
            logger.error(f"Error setting cache value for key {key}: {e}")
            return False

    def delete(self, key: str) -> bool:
        """
        Delete value from cache.

        Args:
            key: Cache key

        Returns:
            True if successfully deleted
        """
        if not self.redis_client:
            return False

        try:
            result = self.redis_client.delete(key)
            logger.debug(f"Deleted cache key: {key}")
            return bool(result)
        except Exception as e:
            logger.error(f"Error deleting cache key {key}: {e}")
            return False

    def delete_pattern(self, pattern: str) -> int:
        """
        Delete all keys matching a pattern.

        Args:
            pattern: Pattern to match (supports wildcards)

        Returns:
            Number of keys deleted
        """
        if not self.redis_client:
            return 0

        try:
            keys = self.redis_client.keys(pattern)
            if keys:
                deleted_count = self.redis_client.delete(*keys)
                logger.info(f"Deleted {deleted_count} keys matching pattern: {pattern}")
                return deleted_count
            return 0
        except Exception as e:
            logger.error(f"Error deleting keys with pattern {pattern}: {e}")
            return 0

    def increment(self, key: str, amount: int = 1) -> Optional[int]:
        """
        Increment a counter in cache.

        Args:
            key: Cache key
            amount: Amount to increment by

        Returns:
            New value after increment
        """
        if not self.redis_client:
            return None

        try:
            return self.redis_client.incrby(key, amount)
        except Exception as e:
            logger.error(f"Error incrementing cache key {key}: {e}")
            return None

    def set_with_ttl(self, key: str, value: Any, ttl: timedelta) -> bool:
        """
        Set value with time-to-live.

        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live as timedelta

        Returns:
            True if successfully cached
        """
        return self.set(key, value, int(ttl.total_seconds()))

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.

        Returns:
            Dictionary with cache statistics
        """
        if not self.redis_client:
            return {"connected": False}

        try:
            info = self.redis_client.info()
            return {
                "connected": True,
                "used_memory": info.get("used_memory_human", "N/A"),
                "connected_clients": info.get("connected_clients", 0),
                "total_commands_processed": info.get("total_commands_processed", 0),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "hit_rate": (
                    info.get("keyspace_hits", 0)
                    / max(
                        info.get("keyspace_hits", 0) + info.get("keyspace_misses", 0), 1
                    )
                )
                * 100,
            }
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {"connected": False, "error": str(e)}


# Global cache service instance
_cache_service: Optional[CacheService] = None


def get_cache_service() -> CacheService:
    """
    Get or create the global cache service instance.

    Returns:
        CacheService instance
    """
    global _cache_service
    if _cache_service is None:
        _cache_service = CacheService()
    return _cache_service


# Cache key generators for different data types
class CacheKeys:
    """Cache key generators for consistent key naming."""

    @staticmethod
    def analytics_revenue(
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        granularity: str = "daily",
        region: Optional[str] = None,
        category: Optional[str] = None,
    ) -> str:
        """Generate cache key for revenue analytics."""
        return f"analytics:revenue:{granularity}:{start_date or 'all'}:{end_date or 'all'}:{region or 'all'}:{category or 'all'}"

    @staticmethod
    def analytics_segments(
        segment_type: str = "rfm", include_predictions: bool = False
    ) -> str:
        """Generate cache key for customer segmentation."""
        return f"analytics:segments:{segment_type}:{include_predictions}"

    @staticmethod
    def analytics_products(
        category: Optional[str] = None,
        sort_by: str = "revenue",
        time_range: str = "30d",
        include_recommendations: bool = False,
    ) -> str:
        """Generate cache key for product performance."""
        return f"analytics:products:{category or 'all'}:{sort_by}:{time_range}:{include_recommendations}"

    @staticmethod
    def analytics_cohort(
        cohort_type: str = "monthly",
        metric: str = "retention",
        start_date: Optional[str] = None,
    ) -> str:
        """Generate cache key for cohort analysis."""
        return f"analytics:cohort:{cohort_type}:{metric}:{start_date or 'all'}"

    @staticmethod
    def analytics_funnel(
        funnel_type: str = "purchase",
        time_range: str = "30d",
        segment: Optional[str] = None,
    ) -> str:
        """Generate cache key for funnel analysis."""
        return f"analytics:funnel:{funnel_type}:{time_range}:{segment or 'all'}"

    @staticmethod
    def customer_list(
        segment: Optional[str] = None,
        risk_level: Optional[str] = None,
        sort_by: str = "lifetime_value",
        page: int = 1,
        size: int = 100,
    ) -> str:
        """Generate cache key for customer list."""
        return f"customers:list:{segment or 'all'}:{risk_level or 'all'}:{sort_by}:{page}:{size}"

    @staticmethod
    def customer_details(customer_id: str) -> str:
        """Generate cache key for customer details."""
        return f"customers:details:{customer_id}"

    @staticmethod
    def customer_recommendations(customer_id: str, rec_type: str) -> str:
        """Generate cache key for customer recommendations."""
        return f"customers:recommendations:{customer_id}:{rec_type}"

    @staticmethod
    def fraud_alerts(
        status: Optional[str] = None,
        priority: Optional[str] = None,
        alert_type: Optional[str] = None,
        page: int = 1,
        size: int = 100,
    ) -> str:
        """Generate cache key for fraud alerts."""
        return f"fraud:alerts:{status or 'all'}:{priority or 'all'}:{alert_type or 'all'}:{page}:{size}"

    @staticmethod
    def fraud_dashboard(time_range: str = "24h") -> str:
        """Generate cache key for fraud dashboard."""
        return f"fraud:dashboard:{time_range}"

    @staticmethod
    def realtime_health() -> str:
        """Generate cache key for system health."""
        return "realtime:health"

    @staticmethod
    def realtime_metrics(stream_type: str = "all", time_window: str = "1h") -> str:
        """Generate cache key for real-time metrics."""
        return f"realtime:metrics:{stream_type}:{time_window}"


def cached(expire_time: Optional[int] = None, key_generator: Optional[Callable] = None):
    """
    Decorator for caching function results.

    Args:
        expire_time: Cache expiration time in seconds
        key_generator: Function to generate cache key from args/kwargs

    Usage:
        @cached(expire_time=300)
        def expensive_function(param1, param2):
            # Expensive computation
            return result
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            cache_service = get_cache_service()

            # Generate cache key
            if key_generator:
                cache_key = key_generator(*args, **kwargs)
            else:
                cache_key = f"{func.__module__}.{func.__name__}:{cache_service._serialize_key(*args, **kwargs)}"

            # Try to get from cache
            cached_result = cache_service.get(cache_key)
            if cached_result is not None:
                return cached_result

            # Execute function and cache result
            if (
                hasattr(func, "__code__") and func.__code__.co_flags & 0x80
            ):  # Check if async
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)

            # Cache the result
            cache_service.set(cache_key, result, expire_time)
            return result

        return wrapper

    return decorator


def cache_clear_pattern(pattern: str) -> int:
    """
    Clear cache entries matching a pattern.

    Args:
        pattern: Pattern to match (supports wildcards)

    Returns:
        Number of entries cleared
    """
    cache_service = get_cache_service()
    return cache_service.delete_pattern(pattern)


def cache_invalidate_analytics():
    """Invalidate all analytics cache entries."""
    return cache_clear_pattern("analytics:*")


def cache_invalidate_customers():
    """Invalidate all customer cache entries."""
    return cache_clear_pattern("customers:*")


def cache_invalidate_fraud():
    """Invalidate all fraud cache entries."""
    return cache_clear_pattern("fraud:*")


def cache_invalidate_realtime():
    """Invalidate all real-time cache entries."""
    return cache_clear_pattern("realtime:*")
