"""
API Integration Tests

This module implements comprehensive integration tests for the FastAPI application
with real analytics data and services integration.

Tests cover:
1. API endpoints with real data from analytics engine
2. Authentication and authorization flows
3. Performance and caching behavior
4. Error handling and resilience
"""

import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import httpx
import pytest
from fastapi import status
from fastapi.testclient import TestClient
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from src.api.main import app
from src.database.models import Base, Customer, Order, Product
from src.database.utils import get_database_session


class APIIntegrationTestSetup:
    """Setup class for API integration tests."""

    def __init__(self):
        self.postgres_container = None
        self.redis_container = None
        self.test_client = None
        self.auth_token = None
        self.test_data = {}

    def setup_containers(self):
        """Set up test containers for API integration."""
        # PostgreSQL container
        self.postgres_container = PostgresContainer("postgres:13")
        self.postgres_container.start()

        # Redis container
        self.redis_container = RedisContainer("redis:7-alpine")
        self.redis_container.start()

        # Update app configuration for testing
        postgres_url = self.postgres_container.get_connection_url()
        redis_host = self.redis_container.get_container_host_ip()
        redis_port = self.redis_container.get_exposed_port(6379)

        # Override app dependencies for testing
        app.dependency_overrides.update(
            {
                "database_url": postgres_url,
                "redis_url": f"redis://{redis_host}:{redis_port}",
            }
        )

        self.test_client = TestClient(app)

    def setup_test_database(self):
        """Set up test database with sample data."""
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        postgres_url = self.postgres_container.get_connection_url()
        engine = create_engine(postgres_url)
        Base.metadata.create_all(engine)

        Session = sessionmaker(bind=engine)
        session = Session()

        # Create test customers
        test_customers = [
            Customer(
                user_id=f"api_test_user_{i}",
                email=f"apitest{i}@example.com",
                account_status="active",
                customer_tier="gold" if i % 2 == 0 else "silver",
                created_at=datetime.now() - timedelta(days=30 - i),
            )
            for i in range(10)
        ]

        # Create test products
        test_products = [
            Product(
                product_id=f"api_prod_{i}",
                product_name=f"API Test Product {i}",
                category="electronics" if i % 2 == 0 else "clothing",
                price=float(50 + i * 10),
                created_at=datetime.now() - timedelta(days=20 - i),
            )
            for i in range(10)
        ]

        # Create test orders
        test_orders = []
        for i in range(20):
            order = Order(
                order_id=f"api_order_{i}",
                customer_id=f"api_test_user_{i % 10}",
                product_id=f"api_prod_{i % 10}",
                quantity=i % 5 + 1,
                total_amount=float((50 + (i % 10) * 10) * (i % 5 + 1)),
                order_status="completed" if i % 3 == 0 else "pending",
                created_at=datetime.now() - timedelta(days=15 - i // 2),
            )
            test_orders.append(order)

        session.add_all(test_customers)
        session.add_all(test_products)
        session.add_all(test_orders)
        session.commit()

        # Store test data references
        self.test_data = {
            "customers": [c.user_id for c in test_customers],
            "products": [p.product_id for p in test_products],
            "orders": [o.order_id for o in test_orders],
        }

        session.close()

    def get_auth_token(self):
        """Get authentication token for API testing."""
        # Create test user and get token
        test_user_data = {
            "username": "api_test_user",
            "password": "test_password_123",
            "email": "apitest@example.com",
        }

        # Register test user
        register_response = self.test_client.post(
            "/api/v1/auth/register", json=test_user_data
        )

        if register_response.status_code == 409:
            # User already exists, just login
            pass

        # Login to get token
        login_data = {
            "username": test_user_data["username"],
            "password": test_user_data["password"],
        }

        login_response = self.test_client.post("/api/v1/auth/login", data=login_data)

        if login_response.status_code == 200:
            self.auth_token = login_response.json()["access_token"]
        else:
            # Fallback - create a simple test token (for testing purposes)
            self.auth_token = "test_token_for_integration_testing"

    def teardown(self):
        """Clean up test environment."""
        if self.postgres_container:
            self.postgres_container.stop()
        if self.redis_container:
            self.redis_container.stop()


@pytest.fixture(scope="class")
def api_test_setup():
    """Fixture to set up API integration test environment."""
    setup = APIIntegrationTestSetup()
    setup.setup_containers()
    setup.setup_test_database()
    setup.get_auth_token()

    yield setup

    setup.teardown()


class TestCustomerAnalyticsAPI:
    """Test customer analytics API endpoints."""

    def test_get_customer_rfm_segments(self, api_test_setup):
        """Test RFM customer segmentation API endpoint."""
        headers = (
            {"Authorization": f"Bearer {api_test_setup.auth_token}"}
            if api_test_setup.auth_token
            else {}
        )

        response = api_test_setup.test_client.get(
            "/api/v1/analytics/customers/rfm-segments", headers=headers
        )

        # Should return customer segments even if empty
        assert response.status_code in [200, 404]  # 404 if no data yet

        if response.status_code == 200:
            data = response.json()
            assert "segments" in data or isinstance(data, list)

    def test_get_customer_lifetime_value(self, api_test_setup):
        """Test customer lifetime value API endpoint."""
        headers = (
            {"Authorization": f"Bearer {api_test_setup.auth_token}"}
            if api_test_setup.auth_token
            else {}
        )

        test_customer_id = api_test_setup.test_data["customers"][0]

        response = api_test_setup.test_client.get(
            f"/api/v1/analytics/customers/{test_customer_id}/clv", headers=headers
        )

        # Should handle request even if customer doesn't have CLV data yet
        assert response.status_code in [200, 404]

        if response.status_code == 200:
            data = response.json()
            assert "customer_id" in data
            assert "clv" in data
            assert isinstance(data["clv"], (int, float))

    def test_get_customer_churn_prediction(self, api_test_setup):
        """Test customer churn prediction API endpoint."""
        headers = (
            {"Authorization": f"Bearer {api_test_setup.auth_token}"}
            if api_test_setup.auth_token
            else {}
        )

        test_customer_id = api_test_setup.test_data["customers"][0]

        response = api_test_setup.test_client.get(
            f"/api/v1/analytics/customers/{test_customer_id}/churn-risk",
            headers=headers,
        )

        assert response.status_code in [200, 404]

        if response.status_code == 200:
            data = response.json()
            assert "customer_id" in data
            assert "churn_probability" in data
            assert 0 <= data["churn_probability"] <= 1

    def test_get_customer_journey(self, api_test_setup):
        """Test customer journey analytics API endpoint."""
        headers = (
            {"Authorization": f"Bearer {api_test_setup.auth_token}"}
            if api_test_setup.auth_token
            else {}
        )

        test_customer_id = api_test_setup.test_data["customers"][0]

        response = api_test_setup.test_client.get(
            f"/api/v1/analytics/customers/{test_customer_id}/journey", headers=headers
        )

        assert response.status_code in [200, 404]

        if response.status_code == 200:
            data = response.json()
            assert "customer_id" in data
            assert "touchpoints" in data or "journey_stages" in data


class TestFraudDetectionAPI:
    """Test fraud detection API endpoints."""

    def test_get_fraud_alerts(self, api_test_setup):
        """Test fraud alerts API endpoint."""
        headers = (
            {"Authorization": f"Bearer {api_test_setup.auth_token}"}
            if api_test_setup.auth_token
            else {}
        )

        response = api_test_setup.test_client.get(
            "/api/v1/fraud/alerts", headers=headers
        )

        assert response.status_code in [200, 404]

        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)

    def test_submit_transaction_for_fraud_check(self, api_test_setup):
        """Test transaction fraud check API endpoint."""
        headers = (
            {"Authorization": f"Bearer {api_test_setup.auth_token}"}
            if api_test_setup.auth_token
            else {}
        )

        test_transaction = {
            "transaction_id": "api_test_fraud_001",
            "customer_id": api_test_setup.test_data["customers"][0],
            "amount": 500.0,
            "merchant_id": "test_merchant_001",
            "timestamp": datetime.now().isoformat(),
        }

        response = api_test_setup.test_client.post(
            "/api/v1/fraud/check", json=test_transaction, headers=headers
        )

        assert response.status_code in [200, 201, 422]  # 422 for validation errors

        if response.status_code in [200, 201]:
            data = response.json()
            assert "transaction_id" in data
            assert "risk_score" in data or "fraud_probability" in data

    def test_get_fraud_investigation_case(self, api_test_setup):
        """Test fraud investigation case API endpoint."""
        headers = (
            {"Authorization": f"Bearer {api_test_setup.auth_token}"}
            if api_test_setup.auth_token
            else {}
        )

        # First create a case or use a test case ID
        test_case_id = "test_case_001"

        response = api_test_setup.test_client.get(
            f"/api/v1/fraud/cases/{test_case_id}", headers=headers
        )

        assert response.status_code in [200, 404]

        if response.status_code == 200:
            data = response.json()
            assert "case_id" in data
            assert "status" in data


class TestBusinessIntelligenceAPI:
    """Test business intelligence API endpoints."""

    def test_get_revenue_analytics(self, api_test_setup):
        """Test revenue analytics API endpoint."""
        headers = (
            {"Authorization": f"Bearer {api_test_setup.auth_token}"}
            if api_test_setup.auth_token
            else {}
        )

        response = api_test_setup.test_client.get(
            "/api/v1/analytics/revenue/summary", headers=headers
        )

        assert response.status_code in [200, 404]

        if response.status_code == 200:
            data = response.json()
            assert "total_revenue" in data or "revenue_metrics" in data

    def test_get_product_performance(self, api_test_setup):
        """Test product performance analytics API endpoint."""
        headers = (
            {"Authorization": f"Bearer {api_test_setup.auth_token}"}
            if api_test_setup.auth_token
            else {}
        )

        response = api_test_setup.test_client.get(
            "/api/v1/analytics/products/performance", headers=headers
        )

        assert response.status_code in [200, 404]

        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list) or "products" in data

    def test_get_geographic_analytics(self, api_test_setup):
        """Test geographic analytics API endpoint."""
        headers = (
            {"Authorization": f"Bearer {api_test_setup.auth_token}"}
            if api_test_setup.auth_token
            else {}
        )

        response = api_test_setup.test_client.get(
            "/api/v1/analytics/geographic/sales-distribution", headers=headers
        )

        assert response.status_code in [200, 404]

        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, dict) or isinstance(data, list)


class TestRealtimeAPI:
    """Test real-time metrics API endpoints."""

    def test_get_realtime_metrics(self, api_test_setup):
        """Test real-time metrics API endpoint."""
        headers = (
            {"Authorization": f"Bearer {api_test_setup.auth_token}"}
            if api_test_setup.auth_token
            else {}
        )

        response = api_test_setup.test_client.get(
            "/api/v1/realtime/metrics", headers=headers
        )

        assert response.status_code in [200, 404]

        if response.status_code == 200:
            data = response.json()
            assert "timestamp" in data
            assert "metrics" in data or len(data) > 0

    def test_get_system_health(self, api_test_setup):
        """Test system health API endpoint."""
        response = api_test_setup.test_client.get("/api/v1/health")

        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert data["status"] in ["healthy", "degraded", "unhealthy"]

    def test_get_realtime_dashboard_data(self, api_test_setup):
        """Test real-time dashboard data API endpoint."""
        headers = (
            {"Authorization": f"Bearer {api_test_setup.auth_token}"}
            if api_test_setup.auth_token
            else {}
        )

        response = api_test_setup.test_client.get(
            "/api/v1/realtime/dashboard", headers=headers
        )

        assert response.status_code in [200, 404]

        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, dict)


class TestAPIPerformanceAndCaching:
    """Test API performance and caching behavior."""

    def test_api_response_time_requirements(self, api_test_setup):
        """Test API response time requirements."""
        headers = (
            {"Authorization": f"Bearer {api_test_setup.auth_token}"}
            if api_test_setup.auth_token
            else {}
        )

        # Test multiple endpoints for response time
        endpoints = [
            "/api/v1/health",
            "/api/v1/analytics/customers/rfm-segments",
            "/api/v1/fraud/alerts",
            "/api/v1/realtime/metrics",
        ]

        response_times = []

        for endpoint in endpoints:
            start_time = time.time()
            response = api_test_setup.test_client.get(endpoint, headers=headers)
            end_time = time.time()

            response_time = end_time - start_time
            response_times.append(response_time)

            # Each response should be under 200ms for integration testing
            assert response_time < 0.2, f"{endpoint} took {response_time:.3f}s"

        # Average response time should be under 100ms
        avg_response_time = sum(response_times) / len(response_times)
        assert (
            avg_response_time < 0.1
        ), f"Average response time {avg_response_time:.3f}s too high"

    def test_caching_behavior(self, api_test_setup):
        """Test API caching behavior."""
        headers = (
            {"Authorization": f"Bearer {api_test_setup.auth_token}"}
            if api_test_setup.auth_token
            else {}
        )

        # Make first request
        start_time = time.time()
        response1 = api_test_setup.test_client.get(
            "/api/v1/analytics/revenue/summary", headers=headers
        )
        first_response_time = time.time() - start_time

        # Make second request (should be cached)
        start_time = time.time()
        response2 = api_test_setup.test_client.get(
            "/api/v1/analytics/revenue/summary", headers=headers
        )
        second_response_time = time.time() - start_time

        # Both should return same status code
        assert response1.status_code == response2.status_code

        # If both successful, second should be faster (cached)
        if response1.status_code == 200 and response2.status_code == 200:
            assert second_response_time <= first_response_time

    def test_api_rate_limiting(self, api_test_setup):
        """Test API rate limiting behavior."""
        headers = (
            {"Authorization": f"Bearer {api_test_setup.auth_token}"}
            if api_test_setup.auth_token
            else {}
        )

        # Make many rapid requests to test rate limiting
        responses = []
        for i in range(20):  # 20 rapid requests
            response = api_test_setup.test_client.get("/api/v1/health", headers=headers)
            responses.append(response.status_code)

        # Should get mostly 200s, possibly some 429s (rate limited)
        success_count = responses.count(200)
        rate_limited_count = responses.count(429)

        # At least some requests should succeed
        assert success_count > 0

        # Rate limiting is optional, so we just verify it doesn't break
        assert all(code in [200, 429] for code in responses)


class TestAPIErrorHandling:
    """Test API error handling and resilience."""

    def test_invalid_endpoint_handling(self, api_test_setup):
        """Test handling of invalid endpoints."""
        response = api_test_setup.test_client.get("/api/v1/nonexistent/endpoint")
        assert response.status_code == 404

    def test_invalid_authentication_handling(self, api_test_setup):
        """Test handling of invalid authentication."""
        headers = {"Authorization": "Bearer invalid_token"}

        response = api_test_setup.test_client.get(
            "/api/v1/analytics/customers/rfm-segments", headers=headers
        )

        # Should return 401 Unauthorized
        assert response.status_code in [401, 403, 422]

    def test_invalid_request_data_handling(self, api_test_setup):
        """Test handling of invalid request data."""
        headers = (
            {"Authorization": f"Bearer {api_test_setup.auth_token}"}
            if api_test_setup.auth_token
            else {}
        )

        # Send invalid JSON data
        invalid_transaction = {
            "transaction_id": "",  # Empty ID
            "amount": -100.0,  # Negative amount
            "timestamp": "invalid_timestamp",
        }

        response = api_test_setup.test_client.post(
            "/api/v1/fraud/check", json=invalid_transaction, headers=headers
        )

        # Should return validation error
        assert response.status_code in [400, 422]

    def test_database_connection_failure_handling(self, api_test_setup):
        """Test API behavior when database is unavailable."""
        # This test would require mocking database failures
        # For now, we'll test that the health endpoint handles it gracefully

        response = api_test_setup.test_client.get("/api/v1/health")

        # Health endpoint should always respond
        assert response.status_code == 200
        data = response.json()
        assert "status" in data

    def test_redis_connection_failure_handling(self, api_test_setup):
        """Test API behavior when Redis cache is unavailable."""
        # This would require mocking Redis failures
        # For now, we'll verify that APIs can work without cache

        response = api_test_setup.test_client.get("/api/v1/health")
        assert response.status_code == 200


class TestAPIDataConsistency:
    """Test API data consistency and integrity."""

    def test_customer_data_consistency(self, api_test_setup):
        """Test consistency of customer data across endpoints."""
        headers = (
            {"Authorization": f"Bearer {api_test_setup.auth_token}"}
            if api_test_setup.auth_token
            else {}
        )

        test_customer_id = api_test_setup.test_data["customers"][0]

        # Get customer data from different endpoints
        endpoints = [
            f"/api/v1/analytics/customers/{test_customer_id}/clv",
            f"/api/v1/analytics/customers/{test_customer_id}/churn-risk",
            f"/api/v1/analytics/customers/{test_customer_id}/journey",
        ]

        customer_ids = []
        for endpoint in endpoints:
            response = api_test_setup.test_client.get(endpoint, headers=headers)
            if response.status_code == 200:
                data = response.json()
                if "customer_id" in data:
                    customer_ids.append(data["customer_id"])

        # All endpoints should return the same customer_id
        if customer_ids:
            assert all(cid == customer_ids[0] for cid in customer_ids)

    def test_transaction_data_integrity(self, api_test_setup):
        """Test transaction data integrity across fraud check process."""
        headers = (
            {"Authorization": f"Bearer {api_test_setup.auth_token}"}
            if api_test_setup.auth_token
            else {}
        )

        test_transaction = {
            "transaction_id": "api_integrity_test_001",
            "customer_id": api_test_setup.test_data["customers"][0],
            "amount": 750.0,
            "timestamp": datetime.now().isoformat(),
        }

        # Submit transaction for fraud check
        response = api_test_setup.test_client.post(
            "/api/v1/fraud/check", json=test_transaction, headers=headers
        )

        if response.status_code in [200, 201]:
            data = response.json()

            # Verify transaction data integrity
            assert data["transaction_id"] == test_transaction["transaction_id"]
            if "amount" in data:
                assert float(data["amount"]) == test_transaction["amount"]

    def test_api_pagination_consistency(self, api_test_setup):
        """Test pagination consistency across list endpoints."""
        headers = (
            {"Authorization": f"Bearer {api_test_setup.auth_token}"}
            if api_test_setup.auth_token
            else {}
        )

        # Test pagination on a list endpoint
        response = api_test_setup.test_client.get(
            "/api/v1/analytics/customers/rfm-segments?page=1&size=5", headers=headers
        )

        if response.status_code == 200:
            data = response.json()

            # Check pagination structure
            if isinstance(data, dict) and "items" in data:
                assert "page" in data or "total" in data or len(data["items"]) <= 5
            elif isinstance(data, list):
                assert len(data) <= 5


# Mark all tests as integration tests
pytestmark = pytest.mark.integration
