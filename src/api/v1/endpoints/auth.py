"""
Authentication endpoints for the E-Commerce Analytics Platform API.

This module provides endpoints for user authentication, token management,
user registration, and API key management.
"""

from datetime import datetime, timedelta, timezone
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

from ...auth.models import (
    User,
    UserCreate,
    UserUpdate,
    Token,
    APIKey,
    APIKeyCreate,
    LoginRequest,
    PasswordChangeRequest,
)
from ...auth.auth import (
    get_current_active_user,
)
from ...auth.security import (
    hash_password,
    verify_password,
)
from ...auth.auth import AuthService, get_auth_service
from ...auth.dependencies import (
    get_current_admin_user,
    get_current_api_key,
    get_api_key_service,
    APIKeyService,
    require_manage_users,
    require_manage_api_keys,
)

router = APIRouter()

# OAuth2 scheme for Swagger UI
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/token")


@router.post("/login", response_model=Token, summary="User Login")
async def login(
    login_data: LoginRequest,
    auth_service: AuthService = Depends(get_auth_service),
):
    """
    Authenticate user and return access token.

    - **username**: Username or email address
    - **password**: User password

    Returns JWT access token for API authentication.
    """
    user = auth_service.authenticate_user(login_data.username, login_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Inactive user account"
        )

    token_data = auth_service.create_user_token(user)
    return Token(**token_data)


@router.post("/token", response_model=Token, summary="OAuth2 Token Endpoint")
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
    auth_service: AuthService = Depends(get_auth_service),
):
    """
    OAuth2 compatible token login endpoint for Swagger UI.

    This endpoint is compatible with OAuth2 password flow and can be used
    by Swagger UI for authentication.
    """
    user = auth_service.authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Inactive user account"
        )

    token_data = auth_service.create_user_token(user)
    return Token(**token_data)


@router.get("/me", response_model=User, summary="Get Current User")
async def get_current_user_info(
    current_user: User = Depends(get_current_active_user),
):
    """
    Get current authenticated user information.

    Returns detailed information about the currently authenticated user.
    """
    return current_user


@router.put("/me", response_model=User, summary="Update Current User")
async def update_current_user(
    user_update: UserUpdate,
    current_user: User = Depends(get_current_active_user),
    auth_service: AuthService = Depends(get_auth_service),
):
    """
    Update current user information.

    Users can update their own profile information excluding role and permissions.
    """
    # Users can only update their own email and full name
    allowed_updates = {
        "email": user_update.email,
        "full_name": user_update.full_name,
    }

    # Remove None values
    allowed_updates = {k: v for k, v in allowed_updates.items() if v is not None}

    if not allowed_updates:
        return current_user

    # TODO: Implement user update in database
    # For now, return the current user with simulated updates
    updated_user = current_user.model_copy(update=allowed_updates)
    return updated_user


@router.post("/change-password", summary="Change Password")
async def change_password(
    password_data: PasswordChangeRequest,
    current_user: User = Depends(get_current_active_user),
    auth_service: AuthService = Depends(get_auth_service),
):
    """
    Change user password.

    Users can change their own password by providing the current password
    and a new password that meets security requirements.
    """
    # Get full user data to verify current password
    user_in_db = auth_service.get_user_by_id(current_user.id)
    if not user_in_db:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    # Verify current password
    if not verify_password(password_data.current_password, user_in_db.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Incorrect current password"
        )

    # Hash new password
    new_hashed_password = hash_password(password_data.new_password)

    # TODO: Update password in database
    # For now, just return success message
    return {"message": "Password changed successfully"}


# Admin-only endpoints for user management
@router.post("/users", response_model=User, summary="Create User")
async def create_user(
    user_create: UserCreate,
    current_user: User = Depends(require_manage_users),
    auth_service: AuthService = Depends(get_auth_service),
):
    """
    Create a new user (Admin only).

    Administrators can create new user accounts with specified roles and permissions.
    """
    # Check if user already exists
    existing_user = auth_service.get_user_by_username(user_create.username)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already registered"
        )

    existing_user = auth_service.get_user_by_email(user_create.email)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )

    # Hash password
    hashed_password = hash_password(user_create.password)

    # TODO: Create user in database
    # For now, return a mock created user
    created_user = User(
        id=999,  # Mock ID
        username=user_create.username,
        email=user_create.email,
        full_name=user_create.full_name,
        is_active=user_create.is_active,
        role=user_create.role,
        permissions=user_create.permissions,
        created_at=datetime.now(timezone.utc),
    )

    return created_user


@router.get("/users", response_model=List[User], summary="List Users")
async def list_users(
    skip: int = 0,
    limit: int = 100,
    current_user: User = Depends(require_manage_users),
    auth_service: AuthService = Depends(get_auth_service),
):
    """
    List all users (Admin only).

    Administrators can view all user accounts with pagination support.
    """
    # TODO: Implement user listing from database
    # For now, return mock users
    mock_users = [
        User(
            id=1,
            username="admin",
            email="admin@example.com",
            full_name="System Administrator",
            is_active=True,
            role="admin",
            permissions=[],
            created_at=datetime.now(timezone.utc),
        ),
        User(
            id=2,
            username="analyst1",
            email="analyst1@example.com",
            full_name="Data Analyst",
            is_active=True,
            role="analyst",
            permissions=[],
            created_at=datetime.now(timezone.utc),
        ),
    ]

    return mock_users[skip : skip + limit]


@router.get("/users/{user_id}", response_model=User, summary="Get User")
async def get_user(
    user_id: int,
    current_user: User = Depends(require_manage_users),
    auth_service: AuthService = Depends(get_auth_service),
):
    """
    Get user by ID (Admin only).

    Administrators can view detailed information about any user account.
    """
    user = auth_service.get_user_by_id(user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    return User(
        id=user.id,
        username=user.username,
        email=user.email,
        full_name=user.full_name,
        is_active=user.is_active,
        role=user.role,
        permissions=user.permissions,
        created_at=user.created_at,
        updated_at=user.updated_at,
        last_login=user.last_login,
    )


@router.put("/users/{user_id}", response_model=User, summary="Update User")
async def update_user(
    user_id: int,
    user_update: UserUpdate,
    current_user: User = Depends(require_manage_users),
    auth_service: AuthService = Depends(get_auth_service),
):
    """
    Update user by ID (Admin only).

    Administrators can update user information including roles and permissions.
    """
    user = auth_service.get_user_by_id(user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    # TODO: Update user in database
    # For now, return user with simulated updates
    update_data = user_update.model_dump(exclude_unset=True)
    updated_user = User(
        id=user.id,
        username=user.username,
        email=update_data.get("email", user.email),
        full_name=update_data.get("full_name", user.full_name),
        is_active=update_data.get("is_active", user.is_active),
        role=update_data.get("role", user.role),
        permissions=update_data.get("permissions", user.permissions),
        created_at=user.created_at,
        updated_at=datetime.now(timezone.utc),
        last_login=user.last_login,
    )

    return updated_user


@router.delete("/users/{user_id}", summary="Delete User")
async def delete_user(
    user_id: int,
    current_user: User = Depends(require_manage_users),
    auth_service: AuthService = Depends(get_auth_service),
):
    """
    Delete user by ID (Admin only).

    Administrators can delete user accounts. Users cannot delete their own account.
    """
    if user_id == current_user.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete your own account"
        )

    user = auth_service.get_user_by_id(user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    # TODO: Delete user from database
    # For now, just return success message
    return {"message": f"User {user_id} deleted successfully"}


# API Key management endpoints
@router.post("/api-keys", response_model=APIKey, summary="Create API Key")
async def create_api_key(
    api_key_create: APIKeyCreate,
    current_user: User = Depends(require_manage_api_keys),
    api_service: APIKeyService = Depends(get_api_key_service),
):
    """
    Create a new API key (Admin only).

    Administrators can create API keys with specific permissions for programmatic access.
    """
    api_key = api_service.create_api_key(
        name=api_key_create.name,
        permissions=api_key_create.permissions,
    )

    # Set expiration if provided
    if api_key_create.expires_at:
        api_key.expires_at = api_key_create.expires_at

    return api_key


@router.get("/api-keys", response_model=List[APIKey], summary="List API Keys")
async def list_api_keys(
    current_user: User = Depends(require_manage_api_keys),
    api_service: APIKeyService = Depends(get_api_key_service),
):
    """
    List all API keys (Admin only).

    Administrators can view all API keys and their permissions.
    """
    # TODO: Implement API key listing from database
    # For now, return keys from in-memory storage
    return list(api_service._api_keys.values())


@router.delete("/api-keys/{api_key}", summary="Revoke API Key")
async def revoke_api_key(
    api_key: str,
    current_user: User = Depends(require_manage_api_keys),
    api_service: APIKeyService = Depends(get_api_key_service),
):
    """
    Revoke an API key (Admin only).

    Administrators can revoke API keys to disable programmatic access.
    """
    success = api_service.revoke_api_key(api_key)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )

    return {"message": "API key revoked successfully"}


@router.get("/api-keys/verify", summary="Verify API Key")
async def verify_api_key(
    current_api_key: APIKey = Depends(get_current_api_key),
):
    """
    Verify API key and return its information.

    This endpoint can be used to verify that an API key is valid and active.
    """
    return {
        "valid": True,
        "name": current_api_key.name,
        "permissions": current_api_key.permissions,
        "created_at": current_api_key.created_at,
        "last_used": current_api_key.last_used,
    }