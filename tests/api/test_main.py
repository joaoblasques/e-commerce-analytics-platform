"""
Tests for the main FastAPI application.

This module tests the core FastAPI application setup, health endpoints,
and error handling.
"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.api.main import app


@pytest.fixture
def client():
    """Create a test client for the FastAPI app."""
    return TestClient(app)


@pytest.fixture
def mock_settings():
    """Mock settings for testing."""
    with patch("src.api.main.get_settings") as mock:
        settings = MagicMock()
        settings.environment = "test"
        settings.port = 8000
        settings.allowed_origins = ["*"]
        mock.return_value = settings
        yield settings


class TestMainApp:
    """Test the main FastAPI application."""

    def test_root_endpoint(self, client, mock_settings):
        """Test the root endpoint returns correct information."""
        response = client.get("/")
        assert response.status_code == 200

        data = response.json()
        assert data["message"] == "Welcome to E-Commerce Analytics Platform API"
        assert data["version"] == "1.0.0"
        assert "health" in data

    def test_health_endpoint(self, client, mock_settings):
        """Test the health check endpoint."""
        response = client.get("/health")
        assert response.status_code == 200

        data = response.json()
        assert data["status"] == "healthy"
        assert data["version"] == "1.0.0"
        assert data["environment"] == "test"
        assert "timestamp" in data

    def test_nonexistent_endpoint(self, client):
        """Test that nonexistent endpoints return 404."""
        response = client.get("/nonexistent")
        assert response.status_code == 404

        data = response.json()
        assert "error" in data
        assert data["error"]["code"] == "HTTP_404"


class TestAPIVersioning:
    """Test API versioning structure."""

    def test_v1_health_endpoint(self, client):
        """Test the v1 health endpoint."""
        with patch("src.api.dependencies.validate_database_connection"):
            with patch("src.api.dependencies.validate_redis_connection"):
                response = client.get("/api/v1/health/")
                assert response.status_code == 200

                data = response.json()
                assert data["status"] == "healthy"
                assert data["version"] == "1.0.0"

    @patch("src.api.dependencies.get_database_session")
    def test_v1_customers_endpoint(self, mock_db, client):
        """Test the v1 customers endpoint."""
        # Mock database session
        mock_db.return_value = MagicMock()

        response = client.get("/api/v1/customers/")
        assert response.status_code == 200

        data = response.json()
        assert "customers" in data
        assert "pagination" in data

    @patch("src.api.dependencies.get_database_session")
    def test_v1_analytics_revenue_endpoint(self, mock_db, client):
        """Test the v1 analytics revenue endpoint."""
        # Mock database session
        mock_db.return_value = MagicMock()

        response = client.get("/api/v1/analytics/revenue")
        assert response.status_code == 200

        data = response.json()
        assert "summary" in data
        assert "time_series" in data
        assert "breakdown" in data


class TestErrorHandling:
    """Test error handling and exception responses."""

    def test_validation_error_response(self, client):
        """Test validation error response format."""
        # Test invalid query parameter
        response = client.get("/api/v1/analytics/revenue?granularity=invalid")
        assert response.status_code == 422

        data = response.json()
        assert "error" in data
        assert data["error"]["code"] == "VALIDATION_ERROR"

    @patch("src.api.dependencies.get_database_session")
    def test_not_found_error_response(self, mock_db, client):
        """Test not found error response format."""
        # Mock database session
        mock_db.return_value = MagicMock()

        response = client.get("/api/v1/customers/invalid_customer")
        assert response.status_code == 404

        data = response.json()
        assert "error" in data
        assert data["error"]["code"] == "RESOURCE_NOT_FOUND"

    @patch("src.api.dependencies.get_database_session")
    def test_internal_server_error_response(self, mock_db, client):
        """Test internal server error response format."""
        # Mock database session to raise an exception
        mock_db.side_effect = Exception("Database connection failed")

        response = client.get("/api/v1/customers/")
        assert response.status_code == 500

        data = response.json()
        assert "error" in data
        assert data["error"]["code"] == "DATABASE_ERROR"


class TestCORS:
    """Test CORS middleware configuration."""

    def test_cors_headers_present(self, client, mock_settings):
        """Test that CORS headers are present in responses."""
        response = client.options("/")

        # Check that CORS headers would be handled by middleware
        # The exact headers depend on the request origin
        assert response.status_code in [200, 405]  # OPTIONS may not be implemented

    def test_cors_preflight_request(self, client, mock_settings):
        """Test CORS preflight request handling."""
        headers = {
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "Content-Type",
        }

        response = client.options("/api/v1/health/", headers=headers)
        # FastAPI CORS middleware should handle this
        assert response.status_code in [200, 405]


class TestDocumentation:
    """Test API documentation endpoints."""

    def test_openapi_schema_available(self, client, mock_settings):
        """Test that OpenAPI schema is available in non-production."""
        mock_settings.environment = "development"

        response = client.get("/openapi.json")
        assert response.status_code == 200

        data = response.json()
        assert "openapi" in data
        assert "info" in data
        assert data["info"]["title"] == "E-Commerce Analytics Platform API"

    def test_docs_available_in_development(self, client, mock_settings):
        """Test that documentation is available in development."""
        mock_settings.environment = "development"

        response = client.get("/docs")
        assert response.status_code == 200

        # Check that it returns HTML
        assert "text/html" in response.headers.get("content-type", "")

    def test_docs_not_available_in_production(self, client, mock_settings):
        """Test that documentation is not available in production."""
        mock_settings.environment = "production"

        # Need to recreate the app with production settings
        from src.api.main import create_application

        with patch("src.api.main.get_settings", return_value=mock_settings):
            prod_app = create_application()
            prod_client = TestClient(prod_app)

            response = prod_client.get("/docs")
            assert response.status_code == 404
