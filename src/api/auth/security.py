"""
Security utilities for password hashing and verification.

This module provides secure password hashing using bcrypt and JWT token
generation utilities for the authentication system.
"""

import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Union

from jose import JWTError, jwt
from passlib.context import CryptContext

from ..config import get_settings

# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# JWT configuration
ALGORITHM = "HS256"


def hash_password(password: str) -> str:
    """
    Hash a password using bcrypt.

    Args:
        password: Plain text password

    Returns:
        Hashed password
    """
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verify a password against its hash.

    Args:
        plain_password: Plain text password
        hashed_password: Hashed password

    Returns:
        True if password matches
    """
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(
    data: Dict[str, Any], expires_delta: Optional[timedelta] = None
) -> str:
    """
    Create a JWT access token.

    Args:
        data: Data to encode in the token
        expires_delta: Token expiration time delta

    Returns:
        Encoded JWT token
    """
    to_encode = data.copy()
    settings = get_settings()

    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(hours=24)

    to_encode.update({"exp": expire, "iat": datetime.now(timezone.utc)})
    encoded_jwt = jwt.encode(to_encode, settings.secret_key, algorithm=ALGORITHM)
    return encoded_jwt


def verify_token(token: str) -> Optional[Dict[str, Any]]:
    """
    Verify and decode a JWT token.

    Args:
        token: JWT token to verify

    Returns:
        Decoded token payload or None if invalid
    """
    try:
        settings = get_settings()
        payload = jwt.decode(token, settings.secret_key, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None


def generate_api_key() -> str:
    """
    Generate a secure API key.

    Returns:
        Random API key string
    """
    return f"ecap_{secrets.token_urlsafe(32)}"


def create_refresh_token(user_id: int) -> str:
    """
    Create a refresh token for the user.

    Args:
        user_id: User ID

    Returns:
        Refresh token
    """
    data = {
        "sub": str(user_id),
        "type": "refresh",
    }
    expires_delta = timedelta(days=30)  # Refresh tokens last 30 days
    return create_access_token(data, expires_delta)


def get_token_expiry(expires_delta: Optional[timedelta] = None) -> int:
    """
    Get token expiry time in seconds.

    Args:
        expires_delta: Token expiration time delta

    Returns:
        Expiry time in seconds from now
    """
    if expires_delta:
        return int(expires_delta.total_seconds())
    else:
        return 24 * 60 * 60  # 24 hours in seconds


def create_password_reset_token(email: str) -> str:
    """
    Create a password reset token.

    Args:
        email: User email

    Returns:
        Password reset token
    """
    data = {
        "sub": email,
        "type": "password_reset",
    }
    expires_delta = timedelta(hours=1)  # Password reset tokens last 1 hour
    return create_access_token(data, expires_delta)


def verify_password_reset_token(token: str) -> Optional[str]:
    """
    Verify a password reset token and extract email.

    Args:
        token: Password reset token

    Returns:
        Email if token is valid, None otherwise
    """
    payload = verify_token(token)
    if payload is None:
        return None

    if payload.get("type") != "password_reset":
        return None

    return payload.get("sub")


def is_token_expired(token: str) -> bool:
    """
    Check if a token is expired.

    Args:
        token: JWT token

    Returns:
        True if token is expired
    """
    payload = verify_token(token)
    if payload is None:
        return True

    exp = payload.get("exp")
    if exp is None:
        return True

    return datetime.fromtimestamp(exp, timezone.utc) < datetime.now(timezone.utc)


def get_token_remaining_time(token: str) -> Optional[timedelta]:
    """
    Get remaining time for a token.

    Args:
        token: JWT token

    Returns:
        Remaining time or None if expired/invalid
    """
    payload = verify_token(token)
    if payload is None:
        return None

    exp = payload.get("exp")
    if exp is None:
        return None

    expiry_time = datetime.fromtimestamp(exp, timezone.utc)
    current_time = datetime.now(timezone.utc)

    if expiry_time < current_time:
        return None

    return expiry_time - current_time
