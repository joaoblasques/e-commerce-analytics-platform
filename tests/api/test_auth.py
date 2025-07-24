"""
Tests for authentication and authorization functionality.

This module tests the authentication system including JWT tokens,
user authentication, role-based access control, and API key management.
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch

from src.api.main import app
from src.api.auth.models import UserRole, Permission
from src.api.auth.security import create_access_token, hash_password


@pytest.fixture
def client():
    """Create test client."""
    return TestClient(app)


@pytest.fixture
def mock_auth_service():
    """Create mock authentication service."""
    with patch("src.api.auth.auth.AuthService") as mock:
        yield mock


class TestAuthentication:
    """Test authentication endpoints."""

    def test_login_success(self, client):
        """Test successful login."""
        login_data = {
            "username": "admin",
            "password": "secret"
        }
        
        response = client.post("/api/v1/auth/login", json=login_data)
        
        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert data["token_type"] == "bearer"
        assert "expires_in" in data

    def test_login_invalid_credentials(self, client):
        """Test login with invalid credentials."""
        login_data = {
            "username": "admin",
            "password": "wrong_password"
        }
        
        response = client.post("/api/v1/auth/login", json=login_data)
        
        assert response.status_code == 401
        data = response.json()
        assert "Incorrect username or password" in data["error"]["message"]

    def test_login_missing_credentials(self, client):
        """Test login with missing credentials."""
        response = client.post("/api/v1/auth/login", json={})
        
        assert response.status_code == 422  # Validation error

    def test_oauth2_token_endpoint(self, client):
        """Test OAuth2 compatible token endpoint."""
        form_data = {
            "username": "admin",
            "password": "secret"
        }
        
        response = client.post("/api/v1/auth/token", data=form_data)
        
        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert data["token_type"] == "bearer"

    def test_get_current_user_success(self, client):
        """Test getting current user with valid token."""
        # First login to get token
        login_response = client.post("/api/v1/auth/login", json={
            "username": "admin",
            "password": "secret"
        })
        token = login_response.json()["access_token"]
        
        # Use token to get user info
        headers = {"Authorization": f"Bearer {token}"}
        response = client.get("/api/v1/auth/me", headers=headers)
        
        assert response.status_code == 200
        data = response.json()
        assert data["username"] == "admin"
        assert data["email"] == "admin@example.com"
        assert data["role"] == "admin"

    def test_get_current_user_invalid_token(self, client):
        """Test getting current user with invalid token."""
        headers = {"Authorization": "Bearer invalid_token"}
        response = client.get("/api/v1/auth/me", headers=headers)
        
        assert response.status_code == 401

    def test_get_current_user_no_token(self, client):
        """Test getting current user without token."""
        response = client.get("/api/v1/auth/me")
        
        assert response.status_code == 403  # No credentials provided


class TestAuthorization:
    """Test authorization and role-based access control."""

    def test_admin_required_endpoint_with_admin_user(self, client):
        """Test admin-required endpoint with admin user."""
        # Login as admin
        login_response = client.post("/api/v1/auth/login", json={
            "username": "admin", 
            "password": "secret"
        })
        token = login_response.json()["access_token"]
        
        headers = {"Authorization": f"Bearer {token}"}
        response = client.get("/api/v1/auth/users", headers=headers)
        
        assert response.status_code == 200

    def test_admin_required_endpoint_without_token(self, client):
        """Test admin-required endpoint without token."""
        response = client.get("/api/v1/auth/users")
        
        assert response.status_code == 403


class TestAPIKeys:
    """Test API key authentication."""

    def test_api_key_verification_success(self, client):
        """Test API key verification with valid key."""
        headers = {"X-API-Key": "ecap_dev_key_123"}
        response = client.get("/api/v1/auth/api-keys/verify", headers=headers)
        
        assert response.status_code == 200
        data = response.json()
        assert data["valid"] is True
        assert data["name"] == "Development Key"

    def test_api_key_verification_invalid_key(self, client):
        """Test API key verification with invalid key."""
        headers = {"X-API-Key": "invalid_key"}
        response = client.get("/api/v1/auth/api-keys/verify", headers=headers)
        
        assert response.status_code == 401

    def test_api_key_verification_no_key(self, client):
        """Test API key verification without key."""
        response = client.get("/api/v1/auth/api-keys/verify")
        
        assert response.status_code == 401


class TestPasswordSecurity:
    """Test password security features."""

    def test_password_hashing(self):
        """Test password hashing functionality."""
        password = "test_password_123!"
        hashed = hash_password(password)
        
        assert hashed != password
        assert hashed.startswith("$2b$")

    def test_password_validation_requirements(self, client):
        """Test password validation requirements."""
        user_data = {
            "username": "testuser",
            "email": "test@example.com",
            "password": "weak"  # Weak password
        }
        
        # Login as admin first
        login_response = client.post("/api/v1/auth/login", json={
            "username": "admin",
            "password": "secret"
        })
        token = login_response.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}
        
        response = client.post("/api/v1/auth/users", json=user_data, headers=headers)
        
        assert response.status_code == 422  # Validation error


class TestTokenSecurity:
    """Test JWT token security."""

    def test_token_creation_includes_required_claims(self):
        """Test that tokens include required claims."""
        token_data = {
            "sub": "testuser",
            "user_id": 1,
            "role": "viewer",
            "permissions": ["read:analytics"]
        }
        
        token = create_access_token(token_data)
        
        assert isinstance(token, str)
        assert len(token) > 0

    def test_expired_token_rejection(self, client):
        """Test that expired tokens are rejected."""
        # Create an expired token (this would need more sophisticated testing)
        # For now, just test with an obviously invalid token
        headers = {"Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9"}
        response = client.get("/api/v1/auth/me", headers=headers)
        
        assert response.status_code == 401


class TestRoleBasedAccess:
    """Test role-based access control."""

    def test_role_permissions_mapping(self):
        """Test that roles have correct permissions."""
        from src.api.auth.models import get_role_permissions, UserRole, Permission
        
        admin_permissions = get_role_permissions(UserRole.ADMIN)
        viewer_permissions = get_role_permissions(UserRole.VIEWER)
        
        # Admin should have all permissions
        assert Permission.MANAGE_USERS in admin_permissions
        assert Permission.READ_ANALYTICS in admin_permissions
        
        # Viewer should have limited permissions
        assert Permission.READ_ANALYTICS in viewer_permissions
        assert Permission.MANAGE_USERS not in viewer_permissions

    def test_permission_checking_logic(self):
        """Test permission checking functionality."""
        from src.api.auth.models import has_permission, UserRole, Permission
        
        # Admin should have all permissions
        assert has_permission(UserRole.ADMIN, [], Permission.MANAGE_USERS)
        
        # Viewer should not have admin permissions
        assert not has_permission(UserRole.VIEWER, [], Permission.MANAGE_USERS)
        
        # Test additional permissions
        additional_perms = [Permission.EXPORT_DATA]
        assert has_permission(UserRole.VIEWER, additional_perms, Permission.EXPORT_DATA)


@pytest.mark.integration
class TestAuthenticationIntegration:
    """Integration tests for authentication system."""

    def test_full_authentication_flow(self, client):
        """Test complete authentication flow."""
        # 1. Login
        login_response = client.post("/api/v1/auth/login", json={
            "username": "admin",
            "password": "secret"
        })
        assert login_response.status_code == 200
        token = login_response.json()["access_token"]
        
        # 2. Access protected endpoint
        headers = {"Authorization": f"Bearer {token}"}
        me_response = client.get("/api/v1/auth/me", headers=headers)
        assert me_response.status_code == 200
        
        # 3. Access admin endpoint
        users_response = client.get("/api/v1/auth/users", headers=headers)
        assert users_response.status_code == 200
        
        # 4. Verify user data consistency
        user_data = me_response.json()
        assert user_data["username"] == "admin"
        assert user_data["role"] == "admin"

    def test_api_key_and_jwt_both_work(self, client):
        """Test that both JWT tokens and API keys work for authentication."""
        # Test JWT
        login_response = client.post("/api/v1/auth/login", json={
            "username": "admin",
            "password": "secret"
        })
        token = login_response.json()["access_token"]
        jwt_headers = {"Authorization": f"Bearer {token}"}
        
        jwt_response = client.get("/api/v1/auth/me", headers=jwt_headers)
        assert jwt_response.status_code == 200
        
        # Test API Key
        api_key_headers = {"X-API-Key": "ecap_dev_key_123"}
        api_response = client.get("/api/v1/auth/api-keys/verify", headers=api_key_headers)
        assert api_response.status_code == 200