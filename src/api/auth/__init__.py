"""
Authentication and authorization module for the E-Commerce Analytics Platform API.

This module provides JWT-based authentication, role-based access control (RBAC),
API key management, and OAuth2 integration capabilities.
"""

from .auth import (
    authenticate_user,
    create_access_token_for_user,
    get_current_active_user,
    get_current_user,
    verify_token_and_get_user,
)
from .dependencies import (
    get_current_admin_user,
    get_current_api_key,
    require_permission,
    require_role,
)
from .models import Token, TokenData, User, UserCreate, UserInDB, UserUpdate
from .security import hash_password, verify_password

__all__ = [
    # Models
    "User",
    "UserCreate",
    "UserInDB",
    "UserUpdate",
    "Token",
    "TokenData",
    # Authentication functions
    "authenticate_user",
    "create_access_token_for_user",
    "get_current_user",
    "get_current_active_user",
    "verify_token_and_get_user",
    # Security functions
    "hash_password",
    "verify_password",
    # Dependencies
    "get_current_admin_user",
    "get_current_api_key",
    "require_role",
    "require_permission",
]