"""
Authentication dependencies for FastAPI endpoints.

This module provides dependency functions for role-based access control,
permission checking, and API key authentication.
"""

import secrets
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import Depends, HTTPException, status, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from .auth import get_current_active_user
from .models import APIKey, Permission, User, UserRole, has_permission


# HTTP Bearer token scheme
security = HTTPBearer()


class RoleChecker:
    """Dependency class for checking user roles."""

    def __init__(self, allowed_roles: List[UserRole]):
        """
        Initialize role checker.

        Args:
            allowed_roles: List of allowed roles
        """
        self.allowed_roles = allowed_roles

    def __call__(self, current_user: User = Depends(get_current_active_user)) -> User:
        """
        Check if current user has required role.

        Args:
            current_user: Current authenticated user

        Returns:
            User if authorized

        Raises:
            HTTPException: If user doesn't have required role
        """
        if current_user.role not in self.allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Operation requires one of these roles: {[role.value for role in self.allowed_roles]}"
            )
        return current_user


class PermissionChecker:
    """Dependency class for checking user permissions."""

    def __init__(self, required_permission: Permission):
        """
        Initialize permission checker.

        Args:
            required_permission: Required permission
        """
        self.required_permission = required_permission

    def __call__(self, current_user: User = Depends(get_current_active_user)) -> User:
        """
        Check if current user has required permission.

        Args:
            current_user: Current authenticated user

        Returns:
            User if authorized

        Raises:
            HTTPException: If user doesn't have required permission
        """
        if not has_permission(
            current_user.role, current_user.permissions, self.required_permission
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Operation requires permission: {self.required_permission.value}"
            )
        return current_user


# Common role dependencies
def require_admin_role(current_user: User = Depends(get_current_active_user)) -> User:
    """
    Require admin role.

    Args:
        current_user: Current user

    Returns:
        User if admin

    Raises:
        HTTPException: If not admin
    """
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin role required"
        )
    return current_user


def require_analyst_or_admin_role(current_user: User = Depends(get_current_active_user)) -> User:
    """
    Require analyst or admin role.

    Args:
        current_user: Current user

    Returns:
        User if analyst or admin

    Raises:
        HTTPException: If not analyst or admin
    """
    if current_user.role not in [UserRole.ANALYST, UserRole.ADMIN]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Analyst or admin role required"
        )
    return current_user


# Permission dependencies
def require_read_analytics(current_user: User = Depends(get_current_active_user)) -> User:
    """Require read analytics permission."""
    checker = PermissionChecker(Permission.READ_ANALYTICS)
    return checker(current_user)


def require_write_analytics(current_user: User = Depends(get_current_active_user)) -> User:
    """Require write analytics permission."""
    checker = PermissionChecker(Permission.WRITE_ANALYTICS)
    return checker(current_user)


def require_read_fraud(current_user: User = Depends(get_current_active_user)) -> User:
    """Require read fraud permission."""
    checker = PermissionChecker(Permission.READ_FRAUD)
    return checker(current_user)


def require_write_fraud(current_user: User = Depends(get_current_active_user)) -> User:
    """Require write fraud permission."""
    checker = PermissionChecker(Permission.WRITE_FRAUD)
    return checker(current_user)


def require_manage_users(current_user: User = Depends(get_current_active_user)) -> User:
    """Require manage users permission."""
    checker = PermissionChecker(Permission.MANAGE_USERS)
    return checker(current_user)


def require_export_data(current_user: User = Depends(get_current_active_user)) -> User:
    """Require export data permission."""
    checker = PermissionChecker(Permission.EXPORT_DATA)
    return checker(current_user)


# Convenience functions
def require_role(*roles: UserRole):
    """
    Create a role requirement dependency.

    Args:
        roles: Required roles

    Returns:
        Role checker dependency
    """
    return RoleChecker(list(roles))


def require_permission(permission: Permission):
    """
    Create a permission requirement dependency.

    Args:
        permission: Required permission

    Returns:
        Permission checker dependency
    """
    return PermissionChecker(permission)


# API Key authentication
class APIKeyService:
    """Service for managing API keys."""

    def __init__(self):
        """Initialize API key service."""
        # TODO: Implement database storage for API keys
        # For now, use in-memory storage for development
        self._api_keys = {
            "ecap_dev_key_123": APIKey(
                key="ecap_dev_key_123",
                name="Development Key",
                is_active=True,
                permissions=[Permission.READ_ANALYTICS, Permission.READ_CUSTOMERS],
                created_at=datetime.now(timezone.utc),
            )
        }

    def get_api_key(self, key: str) -> Optional[APIKey]:
        """
        Get API key by key string.

        Args:
            key: API key string

        Returns:
            API key object or None if not found
        """
        return self._api_keys.get(key)

    def create_api_key(self, name: str, permissions: List[Permission]) -> APIKey:
        """
        Create a new API key.

        Args:
            name: API key name
            permissions: List of permissions

        Returns:
            Created API key
        """
        key = f"ecap_{secrets.token_urlsafe(16)}"
        api_key = APIKey(
            key=key,
            name=name,
            is_active=True,
            permissions=permissions,
            created_at=datetime.now(timezone.utc),
        )
        self._api_keys[key] = api_key
        return api_key

    def revoke_api_key(self, key: str) -> bool:
        """
        Revoke an API key.

        Args:
            key: API key string

        Returns:
            True if revoked successfully
        """
        api_key = self._api_keys.get(key)
        if api_key:
            api_key.is_active = False
            return True
        return False


# Global API key service instance
api_key_service = APIKeyService()


def get_api_key_service() -> APIKeyService:
    """
    Get API key service instance.

    Returns:
        API key service
    """
    return api_key_service


def get_current_api_key(
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
    api_service: APIKeyService = Depends(get_api_key_service),
) -> APIKey:
    """
    Get current API key from header.

    Args:
        x_api_key: API key from header
        api_service: API key service

    Returns:
        API key object

    Raises:
        HTTPException: If API key is invalid or inactive
    """
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    api_key = api_service.get_api_key(x_api_key)
    if not api_key or not api_key.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or inactive API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    # TODO: Update last_used timestamp in database
    api_key.last_used = datetime.now(timezone.utc)

    return api_key


def require_api_key_permission(permission: Permission):
    """
    Create an API key permission requirement dependency.

    Args:
        permission: Required permission

    Returns:
        Dependency function
    """

    def check_api_key_permission(
        api_key: APIKey = Depends(get_current_api_key),
    ) -> APIKey:
        """
        Check if API key has required permission.

        Args:
            api_key: Current API key

        Returns:
            API key if authorized

        Raises:
            HTTPException: If API key doesn't have required permission
        """
        if permission not in api_key.permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"API key missing required permission: {permission.value}"
            )
        return api_key

    return check_api_key_permission


# Combined authentication (JWT or API key)
def get_current_user_or_api_key(
    current_user: Optional[User] = None,
    api_key: Optional[APIKey] = None,
) -> dict:
    """
    Get current authentication context (user or API key).

    Args:
        current_user: Current authenticated user
        api_key: Current API key

    Returns:
        Authentication context

    Raises:
        HTTPException: If no valid authentication provided
    """
    if current_user:
        return {
            "type": "user",
            "user": current_user,
            "permissions": [Permission.READ_ANALYTICS],  # Add user permissions
        }
    elif api_key:
        return {
            "type": "api_key",
            "api_key": api_key,
            "permissions": api_key.permissions,
        }
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required (JWT token or API key)",
        )


# Specific role dependencies with better names
get_current_admin_user = require_admin_role
get_current_analyst_user = require_analyst_or_admin_role