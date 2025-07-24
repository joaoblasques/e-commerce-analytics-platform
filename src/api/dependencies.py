"""
Dependency injection for FastAPI application.

This module provides dependency injection for database connections, Redis clients,
authentication, and other shared resources used across API endpoints.
"""

import logging
from typing import AsyncGenerator, Optional

import redis.asyncio as redis
from fastapi import Depends, HTTPException, Request, status
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from .config import get_database_config, get_redis_config, get_settings
from .exceptions import CacheError, DatabaseError, RateLimitError

logger = logging.getLogger(__name__)

# Global variables for connection management
_database_engine = None
_async_database_engine = None
_redis_client = None
_session_local = None
_async_session_local = None


def get_database_engine():
    """Get or create the database engine."""
    global _database_engine
    if _database_engine is None:
        config = get_database_config()
        _database_engine = create_engine(
            config["url"],
            pool_size=config["pool_size"],
            max_overflow=config["max_overflow"],
            pool_pre_ping=config["pool_pre_ping"],
            pool_recycle=config["pool_recycle"],
            echo=config["echo"],
        )
    return _database_engine


def get_async_database_engine():
    """Get or create the async database engine."""
    global _async_database_engine
    if _async_database_engine is None:
        config = get_database_config()
        # Convert sync URL to async URL
        async_url = config["url"].replace("postgresql://", "postgresql+asyncpg://")
        _async_database_engine = create_async_engine(
            async_url,
            pool_size=config["pool_size"],
            max_overflow=config["max_overflow"],
            pool_pre_ping=config["pool_pre_ping"],
            pool_recycle=config["pool_recycle"],
            echo=config["echo"],
        )
    return _async_database_engine


def get_session_local():
    """Get or create the session local factory."""
    global _session_local
    if _session_local is None:
        _session_local = sessionmaker(
            bind=get_database_engine(),
            autocommit=False,
            autoflush=False,
        )
    return _session_local


def get_async_session_local():
    """Get or create the async session local factory."""
    global _async_session_local
    if _async_session_local is None:
        from sqlalchemy.ext.asyncio import async_sessionmaker

        _async_session_local = async_sessionmaker(
            bind=get_async_database_engine(),
            class_=AsyncSession,
            autocommit=False,
            autoflush=False,
        )
    return _async_session_local


async def get_redis_client():
    """Get or create the Redis client."""
    global _redis_client
    if _redis_client is None:
        config = get_redis_config()
        _redis_client = redis.from_url(
            config["url"],
            decode_responses=config["decode_responses"],
            socket_keepalive=config["socket_keepalive"],
            socket_keepalive_options=config["socket_keepalive_options"],
            health_check_interval=config["health_check_interval"],
        )
    return _redis_client


# Dependency functions for FastAPI
def get_database_session() -> Session:
    """
    Dependency function to get a database session.

    Yields:
        Session: SQLAlchemy database session

    Raises:
        DatabaseError: If database connection fails
    """
    SessionLocal = get_session_local()
    db = SessionLocal()
    try:
        # Test the connection
        db.execute(text("SELECT 1"))
        yield db
    except Exception as e:
        logger.error(f"Database session error: {e}")
        db.rollback()
        raise DatabaseError("Failed to connect to database")
    finally:
        db.close()


async def get_async_database_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency function to get an async database session.

    Yields:
        AsyncSession: SQLAlchemy async database session

    Raises:
        DatabaseError: If database connection fails
    """
    AsyncSessionLocal = get_async_session_local()
    async with AsyncSessionLocal() as session:
        try:
            # Test the connection
            await session.execute(text("SELECT 1"))
            yield session
        except Exception as e:
            logger.error(f"Async database session error: {e}")
            await session.rollback()
            raise DatabaseError("Failed to connect to database")


async def get_redis_dependency():
    """
    Dependency function to get a Redis client.

    Returns:
        redis.Redis: Redis client instance

    Raises:
        CacheError: If Redis connection fails
    """
    try:
        client = await get_redis_client()
        # Test the connection
        await client.ping()
        return client
    except Exception as e:
        logger.error(f"Redis connection error: {e}")
        raise CacheError("Failed to connect to Redis")


# Authentication dependencies (placeholder for future implementation)
def get_current_user(request: Request):
    """
    Dependency function to get the current authenticated user.

    This is a placeholder implementation. In Task 4.1.2, this will be
    replaced with proper JWT authentication.

    Args:
        request: FastAPI request object

    Returns:
        dict: User information (placeholder)
    """
    # For now, return a mock user for development
    settings = get_settings()
    if settings.environment == "development":
        return {
            "id": "dev-user",
            "username": "developer",
            "email": "dev@ecap.com",
            "roles": ["admin"],
        }

    # In production, this would validate JWT token
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required"
    )


def require_admin(current_user=Depends(get_current_user)):
    """
    Dependency function that requires admin role.

    Args:
        current_user: Current authenticated user

    Returns:
        dict: User information if admin

    Raises:
        HTTPException: If user is not admin
    """
    if "admin" not in current_user.get("roles", []):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required"
        )
    return current_user


# Rate limiting dependency
_rate_limiter_storage = {}


async def rate_limiter(
    request: Request, redis_client=Depends(get_redis_dependency)
) -> None:
    """
    Rate limiting dependency.

    Args:
        request: FastAPI request object
        redis_client: Redis client for rate limiting storage

    Raises:
        RateLimitError: If rate limit is exceeded
    """
    settings = get_settings()
    client_ip = request.client.host

    # Create rate limit key
    rate_limit_key = f"rate_limit:{client_ip}"

    try:
        # Check current request count
        current_requests = await redis_client.get(rate_limit_key)

        if current_requests is None:
            # First request, set counter
            await redis_client.setex(rate_limit_key, settings.rate_limit_window, 1)
        elif int(current_requests) >= settings.rate_limit_requests:
            # Rate limit exceeded
            ttl = await redis_client.ttl(rate_limit_key)
            raise RateLimitError(retry_after=ttl if ttl > 0 else 60)
        else:
            # Increment counter
            await redis_client.incr(rate_limit_key)

    except redis.RedisError as e:
        logger.warning(f"Rate limiting Redis error: {e}")
        # Fallback to in-memory rate limiting
        current_time = int(__import__("time").time())
        window_start = current_time - settings.rate_limit_window

        # Clean old entries
        _rate_limiter_storage[client_ip] = [
            timestamp
            for timestamp in _rate_limiter_storage.get(client_ip, [])
            if timestamp > window_start
        ]

        # Check rate limit
        if (
            len(_rate_limiter_storage.get(client_ip, []))
            >= settings.rate_limit_requests
        ):
            raise RateLimitError(retry_after=settings.rate_limit_window)

        # Add current request
        if client_ip not in _rate_limiter_storage:
            _rate_limiter_storage[client_ip] = []
        _rate_limiter_storage[client_ip].append(current_time)


# Pagination dependency
class PaginationParams:
    """Pagination parameters."""

    def __init__(self, page: int = 1, size: int = None):
        settings = get_settings()
        self.page = max(1, page)
        self.size = min(size or settings.default_page_size, settings.max_page_size)
        self.offset = (self.page - 1) * self.size


def get_pagination(page: int = 1, size: Optional[int] = None) -> PaginationParams:
    """
    Dependency function for pagination parameters.

    Args:
        page: Page number (1-based)
        size: Page size

    Returns:
        PaginationParams: Pagination parameters
    """
    return PaginationParams(page, size)


# Health check dependencies
async def validate_database_connection():
    """
    Validate database connection for health checks.

    Raises:
        DatabaseError: If database connection fails
    """
    try:
        engine = get_database_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        raise DatabaseError("Database connection failed")


async def validate_redis_connection():
    """
    Validate Redis connection for health checks.

    Raises:
        CacheError: If Redis connection fails
    """
    try:
        client = await get_redis_client()
        await client.ping()
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        raise CacheError("Redis connection failed")


# Cleanup functions
async def cleanup_connections():
    """Cleanup database and Redis connections."""
    global _database_engine, _async_database_engine, _redis_client

    if _database_engine:
        _database_engine.dispose()
        _database_engine = None

    if _async_database_engine:
        await _async_database_engine.dispose()
        _async_database_engine = None

    if _redis_client:
        await _redis_client.close()
        _redis_client = None


# Development utilities
def get_development_dependencies():
    """Get development-specific dependencies."""
    settings = get_settings()
    if settings.environment != "development":
        return {}

    return {
        "mock_user": get_current_user,
        "skip_rate_limit": lambda: None,
    }
