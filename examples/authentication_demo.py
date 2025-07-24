"""
Authentication and Authorization Demo for E-Commerce Analytics Platform.

This script demonstrates how to use the authentication system including:
- User login and JWT tokens
- API key authentication
- Role-based access control
- Protected endpoint access

Run this script with the FastAPI server running on localhost:8000.
"""

import requests
import json
from typing import Dict, Optional


class ECAPClient:
    """Client for the E-Commerce Analytics Platform API."""

    def __init__(self, base_url: str = "http://localhost:8000"):
        """
        Initialize the client.

        Args:
            base_url: Base URL of the API
        """
        self.base_url = base_url
        self.session = requests.Session()
        self.token: Optional[str] = None

    def login(self, username: str, password: str) -> Dict:
        """
        Login with username and password.

        Args:
            username: Username
            password: Password

        Returns:
            Login response with token information
        """
        url = f"{self.base_url}/api/v1/auth/login"
        data = {
            "username": username,
            "password": password
        }

        response = self.session.post(url, json=data)
        response.raise_for_status()

        token_data = response.json()
        self.token = token_data["access_token"]

        # Set authorization header for future requests
        self.session.headers.update({
            "Authorization": f"Bearer {self.token}"
        })

        return token_data

    def get_current_user(self) -> Dict:
        """
        Get current user information.

        Returns:
            Current user data
        """
        url = f"{self.base_url}/api/v1/auth/me"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def list_users(self) -> list:
        """
        List all users (admin only).

        Returns:
            List of users
        """
        url = f"{self.base_url}/api/v1/auth/users"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def create_api_key(self, name: str, permissions: list) -> Dict:
        """
        Create an API key (admin only).

        Args:
            name: API key name
            permissions: List of permissions

        Returns:
            Created API key data
        """
        url = f"{self.base_url}/api/v1/auth/api-keys"
        data = {
            "name": name,
            "permissions": permissions
        }

        response = self.session.post(url, json=data)
        response.raise_for_status()
        return response.json()

    def verify_api_key(self, api_key: str) -> Dict:
        """
        Verify an API key.

        Args:
            api_key: API key to verify

        Returns:
            API key verification result
        """
        url = f"{self.base_url}/api/v1/auth/api-keys/verify"
        headers = {"X-API-Key": api_key}

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    def get_health_status(self) -> Dict:
        """
        Get API health status (public endpoint).

        Returns:
            Health status
        """
        url = f"{self.base_url}/health"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def logout(self):
        """Logout by clearing the token."""
        self.token = None
        if "Authorization" in self.session.headers:
            del self.session.headers["Authorization"]


def demonstrate_jwt_authentication():
    """Demonstrate JWT token-based authentication."""
    print("🔐 JWT Authentication Demo")
    print("=" * 50)

    client = ECAPClient()

    try:
        # 1. Test public endpoint (no authentication required)
        print("\n1. Testing public endpoint...")
        health = client.get_health_status()
        print(f"✅ Health check: {health['status']}")

        # 2. Login with valid credentials
        print("\n2. Logging in with admin credentials...")
        token_data = client.login("admin", "secret")
        print(f"✅ Login successful!")
        print(f"   Token type: {token_data['token_type']}")
        print(f"   Expires in: {token_data['expires_in']} seconds")

        # 3. Get current user information
        print("\n3. Getting current user information...")
        user = client.get_current_user()
        print(f"✅ Current user: {user['username']} ({user['role']})")
        print(f"   Email: {user['email']}")
        print(f"   Active: {user['is_active']}")

        # 4. Access admin-only endpoint
        print("\n4. Accessing admin-only endpoint...")
        users = client.list_users()
        print(f"✅ Found {len(users)} users in the system")
        for user in users:
            print(f"   - {user['username']} ({user['role']})")

        # 5. Create an API key
        print("\n5. Creating an API key...")
        api_key_data = client.create_api_key(
            name="Demo API Key",
            permissions=["read:analytics", "read:customers"]
        )
        print(f"✅ API key created: {api_key_data['name']}")
        print(f"   Key: {api_key_data['key'][:20]}...")
        print(f"   Permissions: {api_key_data['permissions']}")

        return api_key_data['key']

    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP Error: {e}")
        if e.response:
            print(f"   Response: {e.response.text}")
        return None

    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def demonstrate_api_key_authentication(api_key: str):
    """
    Demonstrate API key authentication.

    Args:
        api_key: API key to test
    """
    print("\n🔑 API Key Authentication Demo")
    print("=" * 50)

    client = ECAPClient()

    try:
        # Verify the API key
        print(f"\n1. Verifying API key: {api_key[:20]}...")
        verification = client.verify_api_key(api_key)
        print(f"✅ API key is valid!")
        print(f"   Name: {verification['name']}")
        print(f"   Permissions: {verification['permissions']}")
        if verification['last_used']:
            print(f"   Last used: {verification['last_used']}")

    except requests.exceptions.HTTPError as e:
        print(f"❌ API key verification failed: {e}")
        if e.response:
            print(f"   Response: {e.response.text}")

    except Exception as e:
        print(f"❌ Error: {e}")


def demonstrate_unauthorized_access():
    """Demonstrate what happens with unauthorized access."""
    print("\n🚫 Unauthorized Access Demo")
    print("=" * 50)

    client = ECAPClient()

    try:
        # Try to access protected endpoint without authentication
        print("\n1. Trying to access protected endpoint without authentication...")
        client.get_current_user()

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print("✅ Access correctly denied (403 Forbidden)")
        elif e.response.status_code == 401:
            print("✅ Authentication required (401 Unauthorized)")
        else:
            print(f"❌ Unexpected error: {e}")

    try:
        # Try to access admin endpoint without proper role
        print("\n2. Trying to access admin endpoint without admin role...")
        # Note: This would require a non-admin user token to properly test
        print("   (Would need non-admin user to fully demonstrate)")

    except Exception as e:
        print(f"❌ Error: {e}")


def demonstrate_invalid_credentials():
    """Demonstrate login with invalid credentials."""
    print("\n❌ Invalid Credentials Demo")
    print("=" * 50)

    client = ECAPClient()

    try:
        print("\n1. Trying to login with invalid credentials...")
        client.login("admin", "wrong_password")

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            print("✅ Login correctly rejected (401 Unauthorized)")
            error_data = e.response.json()
            print(f"   Message: {error_data['error']['message']}")
        else:
            print(f"❌ Unexpected error: {e}")

    except Exception as e:
        print(f"❌ Error: {e}")


def main():
    """Run the authentication demonstration."""
    print("🚀 E-Commerce Analytics Platform - Authentication Demo")
    print("=" * 60)
    print("This demo shows the authentication and authorization features.")
    print("Make sure the FastAPI server is running on localhost:8000")
    print()

    # Test server connection
    try:
        client = ECAPClient()
        health = client.get_health_status()
        print(f"✅ Server is running (status: {health['status']})")
        print(f"   Environment: {health['environment']}")
        print(f"   Version: {health['version']}")
    except Exception as e:
        print(f"❌ Cannot connect to server: {e}")
        print("Please make sure the FastAPI server is running with:")
        print("cd /path/to/project && python -m src.api.run")
        return

    # Run demonstrations
    api_key = demonstrate_jwt_authentication()
    
    if api_key:
        demonstrate_api_key_authentication(api_key)
    
    demonstrate_unauthorized_access()
    demonstrate_invalid_credentials()

    print("\n" + "=" * 60)
    print("🎉 Authentication demo completed!")
    print("\nNext steps:")
    print("1. Try the interactive API docs at http://localhost:8000/docs")
    print("2. Use the 'Authorize' button in Swagger UI to test authentication")
    print("3. Explore other API endpoints with proper authentication")


if __name__ == "__main__":
    main()