"""
Custom exceptions for the FastAPI application.

This module provides custom exception classes with proper error codes,
HTTP status codes, and structured error responses.
"""

from typing import Any, Dict, Optional

from fastapi import status


class ECAPException(Exception):
    """
    Base exception class for ECAP API errors.

    All custom exceptions should inherit from this class to ensure
    consistent error handling and response formatting.
    """

    def __init__(
        self,
        message: str,
        error_code: str,
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
        details: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize ECAP exception.

        Args:
            message: Human-readable error message
            error_code: Machine-readable error code
            status_code: HTTP status code
            details: Additional error details
        """
        self.message = message
        self.error_code = error_code
        self.status_code = status_code
        self.details = details or {}
        super().__init__(self.message)


class ValidationError(ECAPException):
    """Exception raised for validation errors."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=message,
            error_code="VALIDATION_ERROR",
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            details=details,
        )


class NotFoundError(ECAPException):
    """Exception raised when a resource is not found."""

    def __init__(self, resource: str, identifier: str):
        super().__init__(
            message=f"{resource} with identifier '{identifier}' not found",
            error_code="RESOURCE_NOT_FOUND",
            status_code=status.HTTP_404_NOT_FOUND,
            details={"resource": resource, "identifier": identifier},
        )


class AuthenticationError(ECAPException):
    """Exception raised for authentication failures."""

    def __init__(self, message: str = "Authentication failed"):
        super().__init__(
            message=message,
            error_code="AUTHENTICATION_FAILED",
            status_code=status.HTTP_401_UNAUTHORIZED,
        )


class AuthorizationError(ECAPException):
    """Exception raised for authorization failures."""

    def __init__(self, message: str = "Insufficient permissions"):
        super().__init__(
            message=message,
            error_code="AUTHORIZATION_FAILED",
            status_code=status.HTTP_403_FORBIDDEN,
        )


class RateLimitError(ECAPException):
    """Exception raised when rate limit is exceeded."""

    def __init__(self, retry_after: Optional[int] = None):
        message = "Rate limit exceeded"
        details = {}
        if retry_after:
            message += f". Retry after {retry_after} seconds"
            details["retry_after"] = retry_after

        super().__init__(
            message=message,
            error_code="RATE_LIMIT_EXCEEDED",
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            details=details,
        )


class DatabaseError(ECAPException):
    """Exception raised for database-related errors."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=f"Database error: {message}",
            error_code="DATABASE_ERROR",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=details,
        )


class CacheError(ECAPException):
    """Exception raised for cache-related errors."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=f"Cache error: {message}",
            error_code="CACHE_ERROR",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=details,
        )


class SparkError(ECAPException):
    """Exception raised for Spark-related errors."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=f"Spark processing error: {message}",
            error_code="SPARK_ERROR",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=details,
        )


class AnalyticsError(ECAPException):
    """Exception raised for analytics processing errors."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=f"Analytics processing error: {message}",
            error_code="ANALYTICS_ERROR",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=details,
        )


class ConfigurationError(ECAPException):
    """Exception raised for configuration errors."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=f"Configuration error: {message}",
            error_code="CONFIGURATION_ERROR",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=details,
        )


# HTTP status code to exception mapping for common errors
HTTP_EXCEPTION_MAP = {
    status.HTTP_400_BAD_REQUEST: ValidationError,
    status.HTTP_401_UNAUTHORIZED: AuthenticationError,
    status.HTTP_403_FORBIDDEN: AuthorizationError,
    status.HTTP_404_NOT_FOUND: NotFoundError,
    status.HTTP_429_TOO_MANY_REQUESTS: RateLimitError,
}


def create_http_exception(status_code: int, message: str) -> ECAPException:
    """
    Create an appropriate exception based on HTTP status code.

    Args:
        status_code: HTTP status code
        message: Error message

    Returns:
        ECAPException: Appropriate exception instance
    """
    exception_class = HTTP_EXCEPTION_MAP.get(status_code, ECAPException)

    if exception_class == ECAPException:
        return ECAPException(
            message=message, error_code=f"HTTP_{status_code}", status_code=status_code
        )
    else:
        return exception_class(message)


# Utility functions for common error scenarios
def raise_not_found(resource: str, identifier: str) -> None:
    """Raise a NotFoundError."""
    raise NotFoundError(resource, identifier)


def raise_validation_error(
    message: str, details: Optional[Dict[str, Any]] = None
) -> None:
    """Raise a ValidationError."""
    raise ValidationError(message, details)


def raise_authentication_error(message: str = "Authentication failed") -> None:
    """Raise an AuthenticationError."""
    raise AuthenticationError(message)


def raise_authorization_error(message: str = "Insufficient permissions") -> None:
    """Raise an AuthorizationError."""
    raise AuthorizationError(message)


def raise_database_error(
    message: str, details: Optional[Dict[str, Any]] = None
) -> None:
    """Raise a DatabaseError."""
    raise DatabaseError(message, details)


def raise_analytics_error(
    message: str, details: Optional[Dict[str, Any]] = None
) -> None:
    """Raise an AnalyticsError."""
    raise AnalyticsError(message, details)
