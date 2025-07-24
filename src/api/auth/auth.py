"""
Authentication service for user management and token handling.

This module provides the core authentication functions including user
authentication, token creation, and user management operations.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session

from ..dependencies import get_database_session
from .models import TokenData, User, UserInDB
from .security import create_access_token, verify_password, verify_token


# HTTP Bearer token scheme
security = HTTPBearer()


class AuthService:
    """Authentication service class."""

    def __init__(self, db: Session):
        """
        Initialize authentication service.

        Args:
            db: Database session
        """
        self.db = db

    def get_user_by_username(self, username: str) -> Optional[UserInDB]:
        """
        Get user by username from database.

        Args:
            username: Username to search for

        Returns:
            User object or None if not found
        """
        # TODO: Implement database query when user table is created
        # For now, return a mock user for development
        if username == "admin":
            return UserInDB(
                id=1,
                username="admin",
                email="admin@example.com",
                full_name="System Administrator",
                is_active=True,
                role="admin",
                permissions=[],
                hashed_password="$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6Lruj3vjPGga31lW",  # "secret"
                created_at=datetime.now(timezone.utc),
            )
        return None

    def get_user_by_email(self, email: str) -> Optional[UserInDB]:
        """
        Get user by email from database.

        Args:
            email: Email to search for

        Returns:
            User object or None if not found
        """
        # TODO: Implement database query when user table is created
        return None

    def get_user_by_id(self, user_id: int) -> Optional[UserInDB]:
        """
        Get user by ID from database.

        Args:
            user_id: User ID to search for

        Returns:
            User object or None if not found
        """
        # TODO: Implement database query when user table is created
        if user_id == 1:
            return UserInDB(
                id=1,
                username="admin",
                email="admin@example.com",
                full_name="System Administrator",
                is_active=True,
                role="admin",
                permissions=[],
                hashed_password="$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6Lruj3vjPGga31lW",  # "secret"
                created_at=datetime.now(timezone.utc),
            )
        return None

    def authenticate_user(self, username: str, password: str) -> Optional[UserInDB]:
        """
        Authenticate a user with username and password.

        Args:
            username: Username or email
            password: Plain text password

        Returns:
            User object if authentication successful, None otherwise
        """
        # Try to find user by username first, then by email
        user = self.get_user_by_username(username)
        if not user:
            user = self.get_user_by_email(username)

        if not user:
            return None

        if not verify_password(password, user.hashed_password):
            return None

        return user

    def create_user_token(self, user: UserInDB) -> dict:
        """
        Create access token for a user.

        Args:
            user: User object

        Returns:
            Token data dictionary
        """
        from ..config import get_settings
        settings = get_settings()
        
        access_token_expires = timedelta(minutes=settings.access_token_expire_minutes)
        access_token = create_access_token(
            data={
                "sub": user.username,
                "user_id": user.id,
                "role": user.role,
                "permissions": [p.value for p in user.permissions],
            },
            expires_delta=access_token_expires,
        )

        return {
            "access_token": access_token,
            "token_type": "bearer",
            "expires_in": int(access_token_expires.total_seconds()),
        }


# Authentication functions that depend on database session
def get_auth_service(db: Session = Depends(get_database_session)) -> AuthService:
    """
    Get authentication service instance.

    Args:
        db: Database session

    Returns:
        AuthService instance
    """
    return AuthService(db)


def authenticate_user(
    username: str, password: str, auth_service: AuthService = Depends(get_auth_service)
) -> Optional[UserInDB]:
    """
    Authenticate a user.

    Args:
        username: Username or email
        password: Password
        auth_service: Authentication service

    Returns:
        User if authenticated, None otherwise
    """
    return auth_service.authenticate_user(username, password)


def create_access_token_for_user(
    user: UserInDB, auth_service: AuthService = Depends(get_auth_service)
) -> dict:
    """
    Create access token for user.

    Args:
        user: User object
        auth_service: Authentication service

    Returns:
        Token data
    """
    return auth_service.create_user_token(user)


def verify_token_and_get_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    auth_service: AuthService = Depends(get_auth_service),
) -> UserInDB:
    """
    Verify JWT token and get current user.

    Args:
        credentials: HTTP authorization credentials
        auth_service: Authentication service

    Returns:
        Current user

    Raises:
        HTTPException: If token is invalid or user not found
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = verify_token(credentials.credentials)
        if payload is None:
            raise credentials_exception

        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception

        token_data = TokenData(
            username=username,
            user_id=payload.get("user_id"),
            role=payload.get("role"),
            permissions=payload.get("permissions", []),
        )
    except Exception:
        raise credentials_exception

    user = auth_service.get_user_by_username(username)
    if user is None:
        raise credentials_exception

    return user


def get_current_user(
    current_user: UserInDB = Depends(verify_token_and_get_user),
) -> User:
    """
    Get current authenticated user.

    Args:
        current_user: Current user from token verification

    Returns:
        User object without sensitive data
    """
    return User(
        id=current_user.id,
        username=current_user.username,
        email=current_user.email,
        full_name=current_user.full_name,
        is_active=current_user.is_active,
        role=current_user.role,
        permissions=current_user.permissions,
        created_at=current_user.created_at,
        updated_at=current_user.updated_at,
        last_login=current_user.last_login,
    )


def get_current_active_user(
    current_user: User = Depends(get_current_user),
) -> User:
    """
    Get current active user (must be active).

    Args:
        current_user: Current user

    Returns:
        Active user

    Raises:
        HTTPException: If user is inactive
    """
    if not current_user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Inactive user"
        )
    return current_user