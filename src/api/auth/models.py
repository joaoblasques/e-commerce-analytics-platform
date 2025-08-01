"""
Authentication models for user management and token handling.

This module defines Pydantic models for user authentication, authorization,
and token management in the E-Commerce Analytics Platform.
"""

from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, EmailStr, Field, validator


class UserRole(str, Enum):
    """User roles for role-based access control (RBAC)."""

    ADMIN = "admin"
    ANALYST = "analyst"
    VIEWER = "viewer"
    API_USER = "api_user"


class Permission(str, Enum):
    """Permissions for fine-grained access control."""

    # Analytics permissions
    READ_ANALYTICS = "read:analytics"
    WRITE_ANALYTICS = "write:analytics"
    DELETE_ANALYTICS = "delete:analytics"

    # Customer data permissions
    READ_CUSTOMERS = "read:customers"
    WRITE_CUSTOMERS = "write:customers"
    DELETE_CUSTOMERS = "delete:customers"

    # Fraud detection permissions
    READ_FRAUD = "read:fraud"
    WRITE_FRAUD = "write:fraud"
    MANAGE_FRAUD_RULES = "manage:fraud_rules"

    # Business intelligence permissions
    READ_BI = "read:business_intelligence"
    WRITE_BI = "write:business_intelligence"

    # System administration permissions
    MANAGE_USERS = "manage:users"
    MANAGE_API_KEYS = "manage:api_keys"
    MANAGE_SYSTEM = "manage:system"

    # Data export permissions
    EXPORT_DATA = "export:data"


class UserBase(BaseModel):
    """Base user model with common fields."""

    username: str = Field(..., min_length=3, max_length=50, description="Username")
    email: EmailStr = Field(..., description="User email address")
    full_name: Optional[str] = Field(None, max_length=100, description="Full name")
    is_active: bool = Field(default=True, description="Whether the user is active")
    role: UserRole = Field(default=UserRole.VIEWER, description="User role")
    permissions: List[Permission] = Field(
        default_factory=list, description="Additional permissions"
    )

    @validator("username")
    def validate_username(cls, v):
        """Validate username format."""
        if not v.isalnum() and "_" not in v and "-" not in v:
            raise ValueError(
                "Username can only contain letters, numbers, hyphens, and underscores"
            )
        return v.lower()


class UserCreate(UserBase):
    """Model for user creation requests."""

    password: str = Field(
        ..., min_length=8, max_length=128, description="User password"
    )

    @validator("password")
    def validate_password(cls, v):
        """Validate password strength."""
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters long")

        if not any(c.isupper() for c in v):
            raise ValueError("Password must contain at least one uppercase letter")

        if not any(c.islower() for c in v):
            raise ValueError("Password must contain at least one lowercase letter")

        if not any(c.isdigit() for c in v):
            raise ValueError("Password must contain at least one digit")

        if not any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in v):
            raise ValueError("Password must contain at least one special character")

        return v


class UserUpdate(BaseModel):
    """Model for user update requests."""

    email: Optional[EmailStr] = None
    full_name: Optional[str] = Field(None, max_length=100)
    is_active: Optional[bool] = None
    role: Optional[UserRole] = None
    permissions: Optional[List[Permission]] = None


class UserInDB(UserBase):
    """User model as stored in database (with hashed password)."""

    id: int
    hashed_password: str
    created_at: datetime
    updated_at: Optional[datetime] = None
    last_login: Optional[datetime] = None

    class Config:
        """Pydantic configuration."""

        from_attributes = True


class User(UserBase):
    """User model for API responses (without sensitive data)."""

    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    last_login: Optional[datetime] = None

    class Config:
        """Pydantic configuration."""

        from_attributes = True


class Token(BaseModel):
    """JWT token response model."""

    access_token: str = Field(..., description="JWT access token")
    token_type: str = Field(default="bearer", description="Token type")
    expires_in: int = Field(..., description="Token expiration time in seconds")


class TokenData(BaseModel):
    """Token payload data model."""

    username: Optional[str] = None
    user_id: Optional[int] = None
    role: Optional[UserRole] = None
    permissions: List[Permission] = Field(default_factory=list)


class APIKey(BaseModel):
    """API key model for API authentication."""

    key: str = Field(..., description="API key")
    name: str = Field(..., description="API key name")
    is_active: bool = Field(default=True, description="Whether the API key is active")
    permissions: List[Permission] = Field(
        default_factory=list, description="API key permissions"
    )
    created_at: datetime
    expires_at: Optional[datetime] = None
    last_used: Optional[datetime] = None

    class Config:
        """Pydantic configuration."""

        from_attributes = True


class APIKeyCreate(BaseModel):
    """Model for API key creation requests."""

    name: str = Field(..., min_length=1, max_length=100, description="API key name")
    permissions: List[Permission] = Field(
        default_factory=list, description="API key permissions"
    )
    expires_at: Optional[datetime] = Field(None, description="API key expiration time")


class LoginRequest(BaseModel):
    """Login request model."""

    username: str = Field(..., description="Username or email")
    password: str = Field(..., description="Password")


class PasswordChangeRequest(BaseModel):
    """Password change request model."""

    current_password: str = Field(..., description="Current password")
    new_password: str = Field(..., min_length=8, description="New password")

    @validator("new_password")
    def validate_new_password(cls, v):
        """Validate new password strength."""
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters long")

        if not any(c.isupper() for c in v):
            raise ValueError("Password must contain at least one uppercase letter")

        if not any(c.islower() for c in v):
            raise ValueError("Password must contain at least one lowercase letter")

        if not any(c.isdigit() for c in v):
            raise ValueError("Password must contain at least one digit")

        if not any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in v):
            raise ValueError("Password must contain at least one special character")

        return v


# Role-Permission mapping for RBAC
ROLE_PERMISSIONS = {
    UserRole.ADMIN: [
        Permission.READ_ANALYTICS,
        Permission.WRITE_ANALYTICS,
        Permission.DELETE_ANALYTICS,
        Permission.READ_CUSTOMERS,
        Permission.WRITE_CUSTOMERS,
        Permission.DELETE_CUSTOMERS,
        Permission.READ_FRAUD,
        Permission.WRITE_FRAUD,
        Permission.MANAGE_FRAUD_RULES,
        Permission.READ_BI,
        Permission.WRITE_BI,
        Permission.MANAGE_USERS,
        Permission.MANAGE_API_KEYS,
        Permission.MANAGE_SYSTEM,
        Permission.EXPORT_DATA,
    ],
    UserRole.ANALYST: [
        Permission.READ_ANALYTICS,
        Permission.WRITE_ANALYTICS,
        Permission.READ_CUSTOMERS,
        Permission.READ_FRAUD,
        Permission.WRITE_FRAUD,
        Permission.READ_BI,
        Permission.WRITE_BI,
        Permission.EXPORT_DATA,
    ],
    UserRole.VIEWER: [
        Permission.READ_ANALYTICS,
        Permission.READ_CUSTOMERS,
        Permission.READ_FRAUD,
        Permission.READ_BI,
    ],
    UserRole.API_USER: [
        Permission.READ_ANALYTICS,
        Permission.READ_CUSTOMERS,
        Permission.READ_FRAUD,
        Permission.READ_BI,
    ],
}


def get_role_permissions(role: UserRole) -> List[Permission]:
    """
    Get permissions for a given role.

    Args:
        role: User role

    Returns:
        List of permissions for the role
    """
    return ROLE_PERMISSIONS.get(role, [])


def has_permission(
    user_role: UserRole,
    user_permissions: List[Permission],
    required_permission: Permission,
) -> bool:
    """
    Check if a user has a specific permission.

    Args:
        user_role: User's role
        user_permissions: User's additional permissions
        required_permission: Permission to check

    Returns:
        True if user has the permission
    """
    role_permissions = get_role_permissions(user_role)
    all_permissions = set(role_permissions + user_permissions)
    return required_permission in all_permissions
