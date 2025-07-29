"""
API stress testing using Locust framework.

This module provides comprehensive stress testing for FastAPI endpoints
to validate API performance under concurrent load and identify bottlenecks.
"""

import json
import logging
import random
import statistics
import string
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

import jwt
import psutil
import pytest
import requests
from testcontainers.compose import DockerCompose
from testcontainers.postgres import PostgresContainer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class APIPerformanceMetrics:
    """API performance metrics container."""

    endpoint: str
    method: str
    total_requests: int
    successful_requests: int
    failed_requests: int
    rps: float
    average_response_time_ms: float
    p95_response_time_ms: float
    p99_response_time_ms: float
    error_rate_percent: float
    throughput_requests_per_second: float
    concurrent_users: int
    test_duration_seconds: float
    status_code_distribution: Dict[int, int] = field(default_factory=dict)
    error_details: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary for reporting."""
        return {
            "endpoint": self.endpoint,
            "method": self.method,
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "rps": self.rps,
            "average_response_time_ms": self.average_response_time_ms,
            "p95_response_time_ms": self.p95_response_time_ms,
            "p99_response_time_ms": self.p99_response_time_ms,
            "error_rate_percent": self.error_rate_percent,
            "throughput_requests_per_second": self.throughput_requests_per_second,
            "concurrent_users": self.concurrent_users,
            "test_duration_seconds": self.test_duration_seconds,
            "status_code_distribution": self.status_code_distribution,
            "error_details": self.error_details[:10],  # Limit error details
        }


@dataclass
class StressTestResult:
    """Container for individual request results."""

    status_code: int
    response_time_ms: float
    success: bool
    error_message: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


class APIStressTester:
    """FastAPI stress tester with comprehensive load testing capabilities."""

    def __init__(self, base_url: str, auth_token: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.auth_token = auth_token
        self.session = requests.Session()

        # Set up session headers
        if auth_token:
            self.session.headers.update(
                {
                    "Authorization": f"Bearer {auth_token}",
                    "Content-Type": "application/json",
                }
            )

    def create_auth_token(self, user_data: Dict[str, Any]) -> str:
        """Create JWT token for testing authenticated endpoints."""
        # Mock JWT creation for testing
        payload = {
            "user_id": user_data.get("user_id", "test_user"),
            "role": user_data.get("role", "user"),
            "exp": int(time.time()) + 3600,  # 1 hour expiration
        }
        # Use simple encoding for testing (in production, use proper secret)
        return jwt.encode(payload, "test_secret", algorithm="HS256")

    def generate_test_data(self, endpoint: str) -> Dict[str, Any]:
        """Generate realistic test data for different endpoints."""
        data_generators = {
            "/api/v1/auth/login": {
                "username": f"user_{random.randint(1, 1000)}",
                "password": "test_password",
            },
            "/api/v1/customers/analytics": {
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
                "segment": random.choice(["premium", "standard", "basic"]),
            },
            "/api/v1/fraud/detection": {
                "transaction_id": f"txn_{random.randint(1, 100000)}",
                "user_id": f"user_{random.randint(1, 10000)}",
                "amount": round(random.uniform(10, 5000), 2),
            },
            "/api/v1/analytics/revenue": {
                "period": random.choice(["daily", "weekly", "monthly"]),
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
            },
        }

        return data_generators.get(endpoint, {})

    @contextmanager
    def resource_monitor(self):
        """Monitor system resources during stress test."""
        process = psutil.Process()
        cpu_samples = []
        memory_samples = []
        monitoring = True

        def monitor_resources():
            while monitoring:
                try:
                    cpu_samples.append(process.cpu_percent())
                    memory_samples.append(process.memory_info().rss / 1024 / 1024)  # MB
                    time.sleep(0.1)
                except:
                    pass

        monitor_thread = threading.Thread(target=monitor_resources)
        monitor_thread.start()

        try:
            yield cpu_samples, memory_samples
        finally:
            monitoring = False
            monitor_thread.join(timeout=1)

    def single_request(
        self, method: str, endpoint: str, data: Optional[Dict] = None
    ) -> StressTestResult:
        """Execute a single HTTP request and measure performance."""
        url = f"{self.base_url}{endpoint}"
        start_time = time.time()

        try:
            if method.upper() == "GET":
                response = self.session.get(url, params=data, timeout=30)
            elif method.upper() == "POST":
                response = self.session.post(url, json=data, timeout=30)
            elif method.upper() == "PUT":
                response = self.session.put(url, json=data, timeout=30)
            elif method.upper() == "DELETE":
                response = self.session.delete(url, timeout=30)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            response_time_ms = (time.time() - start_time) * 1000

            return StressTestResult(
                status_code=response.status_code,
                response_time_ms=response_time_ms,
                success=200 <= response.status_code < 400,
                timestamp=start_time,
            )

        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            return StressTestResult(
                status_code=0,
                response_time_ms=response_time_ms,
                success=False,
                error_message=str(e),
                timestamp=start_time,
            )

    def concurrent_user_simulation(
        self, method: str, endpoint: str, duration_seconds: int, user_id: int
    ) -> List[StressTestResult]:
        """Simulate a single user making requests for a duration."""
        results = []
        end_time = time.time() + duration_seconds

        while time.time() < end_time:
            # Generate test data for this request
            data = self.generate_test_data(endpoint)

            # Execute request
            result = self.single_request(method, endpoint, data)
            results.append(result)

            # Brief pause between requests (simulate user think time)
            time.sleep(random.uniform(0.1, 0.5))

        return results

    def stress_test_endpoint(
        self,
        method: str,
        endpoint: str,
        concurrent_users: int,
        duration_seconds: int,
        ramp_up_seconds: int = 0,
    ) -> APIPerformanceMetrics:
        """
        Run stress test on a specific endpoint.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint path
            concurrent_users: Number of concurrent users
            duration_seconds: Test duration in seconds
            ramp_up_seconds: Time to gradually increase load

        Returns:
            APIPerformanceMetrics with test results
        """
        logger.info(
            f"Starting stress test: {method} {endpoint} - "
            f"{concurrent_users} users for {duration_seconds}s"
        )

        all_results = []

        with self.resource_monitor() as (cpu_samples, memory_samples):
            start_time = time.time()

            with ThreadPoolExecutor(max_workers=concurrent_users) as executor:
                # Submit user simulation tasks
                futures = []

                for user_id in range(concurrent_users):
                    # Stagger user start times for ramp-up
                    if ramp_up_seconds > 0:
                        delay = (user_id / concurrent_users) * ramp_up_seconds
                        time.sleep(delay / concurrent_users)  # Distribute the delay

                    future = executor.submit(
                        self.concurrent_user_simulation,
                        method,
                        endpoint,
                        duration_seconds,
                        user_id,
                    )
                    futures.append(future)

                # Collect all results
                for future in as_completed(futures):
                    try:
                        user_results = future.result()
                        all_results.extend(user_results)
                    except Exception as e:
                        logger.error(f"User simulation failed: {e}")

            total_test_time = time.time() - start_time

        # Calculate metrics
        return self._calculate_metrics(
            method, endpoint, all_results, concurrent_users, total_test_time
        )

    def _calculate_metrics(
        self,
        method: str,
        endpoint: str,
        results: List[StressTestResult],
        concurrent_users: int,
        test_duration: float,
    ) -> APIPerformanceMetrics:
        """Calculate performance metrics from test results."""
        if not results:
            return APIPerformanceMetrics(
                endpoint=endpoint,
                method=method,
                total_requests=0,
                successful_requests=0,
                failed_requests=0,
                rps=0,
                average_response_time_ms=0,
                p95_response_time_ms=0,
                p99_response_time_ms=0,
                error_rate_percent=100,
                throughput_requests_per_second=0,
                concurrent_users=concurrent_users,
                test_duration_seconds=test_duration,
            )

        # Basic counts
        total_requests = len(results)
        successful_requests = sum(1 for r in results if r.success)
        failed_requests = total_requests - successful_requests

        # Response times
        response_times = [r.response_time_ms for r in results]
        avg_response_time = statistics.mean(response_times)

        if len(response_times) > 1:
            sorted_times = sorted(response_times)
            p95_index = int(0.95 * len(sorted_times))
            p99_index = int(0.99 * len(sorted_times))
            p95_response_time = sorted_times[p95_index]
            p99_response_time = sorted_times[p99_index]
        else:
            p95_response_time = avg_response_time
            p99_response_time = avg_response_time

        # Rates and percentages
        error_rate = (failed_requests / total_requests) * 100
        throughput = total_requests / test_duration if test_duration > 0 else 0
        rps = throughput  # Same as throughput for this context

        # Status code distribution
        status_codes = {}
        error_details = []

        for result in results:
            status_codes[result.status_code] = (
                status_codes.get(result.status_code, 0) + 1
            )
            if not result.success and result.error_message:
                error_details.append(result.error_message)

        return APIPerformanceMetrics(
            endpoint=endpoint,
            method=method,
            total_requests=total_requests,
            successful_requests=successful_requests,
            failed_requests=failed_requests,
            rps=rps,
            average_response_time_ms=avg_response_time,
            p95_response_time_ms=p95_response_time,
            p99_response_time_ms=p99_response_time,
            error_rate_percent=error_rate,
            throughput_requests_per_second=throughput,
            concurrent_users=concurrent_users,
            test_duration_seconds=test_duration,
            status_code_distribution=status_codes,
            error_details=error_details,
        )

    def load_test_scenario(
        self, scenarios: List[Dict[str, Any]]
    ) -> Dict[str, APIPerformanceMetrics]:
        """
        Run multiple load test scenarios.

        Args:
            scenarios: List of test scenarios with endpoint, method, users, duration

        Returns:
            Dictionary with metrics for each scenario
        """
        results = {}

        for i, scenario in enumerate(scenarios):
            scenario_name = f"scenario_{i+1}_{scenario.get('name', 'unnamed')}"
            logger.info(f"Running load test scenario: {scenario_name}")

            try:
                metrics = self.stress_test_endpoint(
                    method=scenario["method"],
                    endpoint=scenario["endpoint"],
                    concurrent_users=scenario["concurrent_users"],
                    duration_seconds=scenario["duration_seconds"],
                    ramp_up_seconds=scenario.get("ramp_up_seconds", 0),
                )

                results[scenario_name] = metrics

                logger.info(
                    f"Scenario {scenario_name}: "
                    f"{metrics.throughput_requests_per_second:.1f} req/s, "
                    f"{metrics.average_response_time_ms:.1f}ms avg, "
                    f"{metrics.error_rate_percent:.1f}% errors"
                )

                # Cool down between scenarios
                time.sleep(2)

            except Exception as e:
                logger.error(f"Scenario {scenario_name} failed: {e}")
                results[scenario_name] = None

        return results

    def spike_test(
        self,
        method: str,
        endpoint: str,
        normal_users: int,
        spike_users: int,
        spike_duration: int,
        total_duration: int,
    ) -> Dict[str, APIPerformanceMetrics]:
        """
        Run spike test to check behavior under sudden load increase.

        Args:
            method: HTTP method
            endpoint: API endpoint
            normal_users: Normal concurrent users
            spike_users: Peak concurrent users during spike
            spike_duration: Duration of spike in seconds
            total_duration: Total test duration in seconds

        Returns:
            Dictionary with metrics for normal and spike periods
        """
        logger.info(f"Starting spike test: {normal_users} -> {spike_users} users")

        results = {}

        # Phase 1: Normal load
        logger.info("Phase 1: Normal load")
        normal_metrics = self.stress_test_endpoint(
            method, endpoint, normal_users, (total_duration - spike_duration) // 2
        )
        results["normal_load"] = normal_metrics

        # Phase 2: Spike load
        logger.info("Phase 2: Spike load")
        spike_metrics = self.stress_test_endpoint(
            method, endpoint, spike_users, spike_duration
        )
        results["spike_load"] = spike_metrics

        # Phase 3: Recovery load
        logger.info("Phase 3: Recovery load")
        recovery_metrics = self.stress_test_endpoint(
            method, endpoint, normal_users, (total_duration - spike_duration) // 2
        )
        results["recovery_load"] = recovery_metrics

        return results

    def endurance_test(
        self,
        method: str,
        endpoint: str,
        concurrent_users: int,
        duration_minutes: int,
        sample_interval_seconds: int = 60,
    ) -> List[APIPerformanceMetrics]:
        """
        Run endurance test to check performance degradation over time.

        Args:
            method: HTTP method
            endpoint: API endpoint
            concurrent_users: Concurrent users
            duration_minutes: Test duration in minutes
            sample_interval_seconds: Interval between samples

        Returns:
            List of metrics for each sample interval
        """
        logger.info(f"Starting endurance test: {duration_minutes} minutes")

        results = []
        total_duration = duration_minutes * 60
        intervals = total_duration // sample_interval_seconds

        for interval in range(intervals):
            logger.info(f"Endurance test interval {interval + 1}/{intervals}")

            metrics = self.stress_test_endpoint(
                method, endpoint, concurrent_users, sample_interval_seconds
            )

            results.append(metrics)

            # Check for performance degradation
            if len(results) > 1:
                current_response_time = metrics.average_response_time_ms
                initial_response_time = results[0].average_response_time_ms
                degradation = (current_response_time / initial_response_time - 1) * 100

                if degradation > 50:  # 50% degradation threshold
                    logger.warning(
                        f"Performance degradation detected: {degradation:.1f}%"
                    )

        return results


# Test fixtures
@pytest.fixture(scope="session")
def postgres_container():
    """PostgreSQL container for API testing."""
    with PostgresContainer("postgres:13") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def api_server(postgres_container):
    """Start API server with test database."""
    # In a real scenario, this would start the FastAPI server
    # For now, we'll assume it's running on localhost:8000
    base_url = "http://localhost:8000"

    # Wait for server to be ready
    for _ in range(30):
        try:
            response = requests.get(f"{base_url}/health", timeout=1)
            if response.status_code == 200:
                break
        except:
            pass
        time.sleep(1)
    else:
        pytest.skip("API server not available for stress testing")

    yield base_url


@pytest.fixture
def stress_tester(api_server):
    """API stress tester instance."""
    return APIStressTester(api_server)


@pytest.fixture
def authenticated_tester(api_server):
    """Authenticated API stress tester."""
    tester = APIStressTester(api_server)

    # Create test auth token
    token = tester.create_auth_token({"user_id": "stress_test_user", "role": "admin"})

    return APIStressTester(api_server, token)


class TestAPIStressPerformance:
    """API stress performance tests."""

    def test_health_endpoint_stress(self, stress_tester):
        """Test health endpoint under stress."""
        metrics = stress_tester.stress_test_endpoint(
            method="GET",
            endpoint="/health",
            concurrent_users=50,
            duration_seconds=30,
            ramp_up_seconds=5,
        )

        # Health endpoint should handle high load easily
        assert (
            metrics.error_rate_percent < 1.0
        ), f"Health endpoint error rate too high: {metrics.error_rate_percent}%"
        assert (
            metrics.average_response_time_ms < 50
        ), f"Health endpoint too slow: {metrics.average_response_time_ms}ms"
        assert (
            metrics.throughput_requests_per_second > 100
        ), f"Health endpoint throughput too low: {metrics.throughput_requests_per_second}"

        logger.info(
            f"Health endpoint stress test: {metrics.throughput_requests_per_second:.1f} req/s, "
            f"{metrics.average_response_time_ms:.1f}ms avg"
        )

    def test_analytics_endpoint_stress(self, authenticated_tester):
        """Test analytics endpoints under stress."""
        scenarios = [
            {
                "name": "customer_analytics_light",
                "method": "GET",
                "endpoint": "/api/v1/customers/analytics",
                "concurrent_users": 10,
                "duration_seconds": 20,
            },
            {
                "name": "customer_analytics_medium",
                "method": "GET",
                "endpoint": "/api/v1/customers/analytics",
                "concurrent_users": 25,
                "duration_seconds": 20,
            },
            {
                "name": "revenue_analytics",
                "method": "GET",
                "endpoint": "/api/v1/analytics/revenue",
                "concurrent_users": 15,
                "duration_seconds": 20,
            },
        ]

        results = authenticated_tester.load_test_scenario(scenarios)

        # Check that all scenarios completed
        successful_scenarios = [r for r in results.values() if r is not None]
        assert (
            len(successful_scenarios) >= 2
        ), "Not enough scenarios completed successfully"

        # Analytics endpoints should handle reasonable load
        for scenario_name, metrics in results.items():
            if metrics:
                assert (
                    metrics.error_rate_percent < 5.0
                ), f"{scenario_name} error rate too high: {metrics.error_rate_percent}%"
                assert (
                    metrics.average_response_time_ms < 2000
                ), f"{scenario_name} too slow: {metrics.average_response_time_ms}ms"

                logger.info(
                    f"{scenario_name}: {metrics.throughput_requests_per_second:.1f} req/s, "
                    f"{metrics.average_response_time_ms:.1f}ms avg"
                )

    def test_fraud_detection_endpoint_stress(self, authenticated_tester):
        """Test fraud detection endpoint under stress."""
        metrics = authenticated_tester.stress_test_endpoint(
            method="POST",
            endpoint="/api/v1/fraud/detection",
            concurrent_users=20,
            duration_seconds=30,
            ramp_up_seconds=5,
        )

        # Fraud detection should be responsive for real-time use
        assert (
            metrics.error_rate_percent < 5.0
        ), f"Fraud detection error rate too high: {metrics.error_rate_percent}%"
        assert (
            metrics.average_response_time_ms < 500
        ), f"Fraud detection too slow: {metrics.average_response_time_ms}ms"
        assert (
            metrics.p95_response_time_ms < 1000
        ), f"Fraud detection P95 too slow: {metrics.p95_response_time_ms}ms"

        logger.info(
            f"Fraud detection stress test: {metrics.throughput_requests_per_second:.1f} req/s, "
            f"{metrics.average_response_time_ms:.1f}ms avg, "
            f"{metrics.p95_response_time_ms:.1f}ms P95"
        )

    def test_authentication_endpoint_stress(self, stress_tester):
        """Test authentication endpoint under stress."""
        metrics = stress_tester.stress_test_endpoint(
            method="POST",
            endpoint="/api/v1/auth/login",
            concurrent_users=30,
            duration_seconds=25,
        )

        # Auth endpoint critical for user experience
        assert (
            metrics.error_rate_percent < 10.0
        ), f"Auth error rate too high: {metrics.error_rate_percent}%"
        assert (
            metrics.average_response_time_ms < 1000
        ), f"Auth too slow: {metrics.average_response_time_ms}ms"

        logger.info(
            f"Authentication stress test: {metrics.throughput_requests_per_second:.1f} req/s, "
            f"{metrics.average_response_time_ms:.1f}ms avg"
        )

    def test_api_spike_handling(self, authenticated_tester):
        """Test API behavior under traffic spikes."""
        results = authenticated_tester.spike_test(
            method="GET",
            endpoint="/api/v1/customers/analytics",
            normal_users=10,
            spike_users=50,
            spike_duration=15,
            total_duration=60,
        )

        normal_metrics = results["normal_load"]
        spike_metrics = results["spike_load"]
        recovery_metrics = results["recovery_load"]

        # System should handle spike gracefully
        assert (
            spike_metrics.error_rate_percent < 15.0
        ), f"Spike error rate too high: {spike_metrics.error_rate_percent}%"

        # Recovery should be close to normal performance
        response_time_ratio = (
            recovery_metrics.average_response_time_ms
            / normal_metrics.average_response_time_ms
        )
        assert (
            response_time_ratio < 2.0
        ), f"System did not recover well from spike: {response_time_ratio:.2f}x slower"

        logger.info(f"Spike test results:")
        logger.info(f"  Normal: {normal_metrics.average_response_time_ms:.1f}ms avg")
        logger.info(f"  Spike: {spike_metrics.average_response_time_ms:.1f}ms avg")
        logger.info(
            f"  Recovery: {recovery_metrics.average_response_time_ms:.1f}ms avg"
        )

    def test_api_endurance(self, authenticated_tester):
        """Test API performance over extended period."""
        # Run for 5 minutes with 1-minute samples
        results = authenticated_tester.endurance_test(
            method="GET",
            endpoint="/api/v1/analytics/revenue",
            concurrent_users=15,
            duration_minutes=5,
            sample_interval_seconds=60,
        )

        assert len(results) >= 3, "Endurance test did not run long enough"

        # Check for performance degradation
        initial_response_time = results[0].average_response_time_ms
        final_response_time = results[-1].average_response_time_ms

        degradation_percent = (final_response_time / initial_response_time - 1) * 100
        assert (
            degradation_percent < 30
        ), f"Performance degraded too much: {degradation_percent:.1f}%"

        # Check error rates remain low
        for i, metrics in enumerate(results):
            assert (
                metrics.error_rate_percent < 5.0
            ), f"Error rate too high in interval {i+1}: {metrics.error_rate_percent}%"

        logger.info(f"Endurance test results:")
        for i, metrics in enumerate(results):
            logger.info(
                f"  Interval {i+1}: {metrics.average_response_time_ms:.1f}ms avg, "
                f"{metrics.error_rate_percent:.1f}% errors"
            )

    def test_concurrent_endpoint_stress(self, authenticated_tester):
        """Test multiple endpoints under concurrent stress."""
        import threading

        def stress_test_worker(endpoint, method, users, duration, results_dict, key):
            try:
                metrics = authenticated_tester.stress_test_endpoint(
                    method=method,
                    endpoint=endpoint,
                    concurrent_users=users,
                    duration_seconds=duration,
                )
                results_dict[key] = metrics
            except Exception as e:
                logger.error(f"Concurrent stress test failed for {key}: {e}")
                results_dict[key] = None

        # Test multiple endpoints concurrently
        results = {}
        threads = []

        test_configs = [
            ("analytics", "/api/v1/customers/analytics", "GET", 10, 20),
            ("fraud", "/api/v1/fraud/detection", "POST", 8, 20),
            ("revenue", "/api/v1/analytics/revenue", "GET", 12, 20),
        ]

        for name, endpoint, method, users, duration in test_configs:
            thread = threading.Thread(
                target=stress_test_worker,
                args=(endpoint, method, users, duration, results, name),
            )
            threads.append(thread)
            thread.start()

        # Wait for all tests to complete
        for thread in threads:
            thread.join(timeout=60)

        # Validate results
        successful_tests = [r for r in results.values() if r is not None]
        assert len(successful_tests) >= 2, "Not enough concurrent tests completed"

        # Check that concurrent load didn't cause excessive failures
        for test_name, metrics in results.items():
            if metrics:
                assert (
                    metrics.error_rate_percent < 10.0
                ), f"Concurrent test {test_name} error rate too high"
                logger.info(
                    f"Concurrent {test_name}: {metrics.throughput_requests_per_second:.1f} req/s, "
                    f"{metrics.average_response_time_ms:.1f}ms avg"
                )


if __name__ == "__main__":
    # Example usage for standalone testing
    logging.basicConfig(level=logging.INFO)

    print("API Stress Testing Framework")
    print("Run with pytest for full test suite:")
    print("pytest tests/performance/test_api_stress.py -v --tb=short")
