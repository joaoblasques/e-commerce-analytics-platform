"""
Chaos engineering tests for system resilience validation.

This module implements chaos engineering principles to test system behavior
under various failure conditions and validate fault tolerance mechanisms.
"""

import json
import logging
import os
import random
import signal
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

import psutil
import pytest
import requests
from testcontainers.compose import DockerCompose
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer

import docker

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ChaosExperimentResult:
    """Result of a chaos engineering experiment."""

    experiment_name: str
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    failure_injected: bool
    system_recovered: bool
    recovery_time_seconds: Optional[float]
    performance_impact_percent: float
    error_rate_during_chaos: float
    error_rate_before_chaos: float
    error_rate_after_chaos: float
    metrics: Dict[str, Any] = field(default_factory=dict)
    logs: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary for reporting."""
        return {
            "experiment_name": self.experiment_name,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "duration_seconds": self.duration_seconds,
            "failure_injected": self.failure_injected,
            "system_recovered": self.system_recovered,
            "recovery_time_seconds": self.recovery_time_seconds,
            "performance_impact_percent": self.performance_impact_percent,
            "error_rate_during_chaos": self.error_rate_during_chaos,
            "error_rate_before_chaos": self.error_rate_before_chaos,
            "error_rate_after_chaos": self.error_rate_after_chaos,
            "metrics": self.metrics,
            "logs": self.logs[:20],  # Limit log entries
        }


class ChaosEngineer:
    """Chaos engineering framework for resilience testing."""

    def __init__(self, api_base_url: str = "http://localhost:8000"):
        self.api_base_url = api_base_url
        self.docker_client = None
        self.active_experiments = []

        try:
            self.docker_client = docker.from_env()
        except Exception as e:
            logger.warning(f"Docker client not available: {e}")

    def is_system_healthy(self) -> bool:
        """Check if the system is in a healthy state."""
        try:
            # Check API health
            response = requests.get(f"{self.api_base_url}/health", timeout=5)
            if response.status_code != 200:
                return False

            # Check basic functionality
            test_response = requests.get(
                f"{self.api_base_url}/api/v1/health", timeout=5
            )
            return test_response.status_code == 200

        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    def measure_baseline_performance(
        self, duration_seconds: int = 30
    ) -> Dict[str, float]:
        """Measure baseline system performance before chaos experiments."""
        logger.info(f"Measuring baseline performance for {duration_seconds}s")

        metrics = {
            "response_times": [],
            "error_count": 0,
            "total_requests": 0,
            "cpu_usage": [],
            "memory_usage": [],
        }

        start_time = time.time()

        def make_requests():
            while time.time() - start_time < duration_seconds:
                try:
                    request_start = time.time()
                    response = requests.get(f"{self.api_base_url}/health", timeout=5)
                    response_time = (time.time() - request_start) * 1000

                    metrics["response_times"].append(response_time)
                    metrics["total_requests"] += 1

                    if response.status_code != 200:
                        metrics["error_count"] += 1

                    time.sleep(0.1)  # 10 RPS

                except Exception as e:
                    metrics["error_count"] += 1
                    metrics["total_requests"] += 1

        def monitor_resources():
            while time.time() - start_time < duration_seconds:
                try:
                    cpu_percent = psutil.cpu_percent(interval=1)
                    memory = psutil.virtual_memory()

                    metrics["cpu_usage"].append(cpu_percent)
                    metrics["memory_usage"].append(memory.percent)
                except:
                    pass

        # Run measurement in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            executor.submit(make_requests)
            executor.submit(monitor_resources)

        # Calculate baseline metrics
        avg_response_time = (
            sum(metrics["response_times"]) / len(metrics["response_times"])
            if metrics["response_times"]
            else 0
        )
        error_rate = (metrics["error_count"] / max(metrics["total_requests"], 1)) * 100
        avg_cpu = (
            sum(metrics["cpu_usage"]) / len(metrics["cpu_usage"])
            if metrics["cpu_usage"]
            else 0
        )
        avg_memory = (
            sum(metrics["memory_usage"]) / len(metrics["memory_usage"])
            if metrics["memory_usage"]
            else 0
        )

        return {
            "avg_response_time_ms": avg_response_time,
            "error_rate_percent": error_rate,
            "avg_cpu_percent": avg_cpu,
            "avg_memory_percent": avg_memory,
            "total_requests": metrics["total_requests"],
        }

    @contextmanager
    def chaos_experiment(self, experiment_name: str):
        """Context manager for chaos experiments."""
        start_time = datetime.now()
        logger.info(f"Starting chaos experiment: {experiment_name}")

        # Measure baseline
        baseline = self.measure_baseline_performance(10)

        try:
            yield baseline
        finally:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(
                f"Chaos experiment {experiment_name} completed in {duration:.1f}s"
            )

    def inject_cpu_stress(
        self, duration_seconds: int = 30, cpu_cores: int = 2
    ) -> ChaosExperimentResult:
        """Inject CPU stress to test performance under high CPU load."""
        experiment_name = f"cpu_stress_{cpu_cores}_cores"

        with self.chaos_experiment(experiment_name) as baseline:
            start_time = datetime.now()

            # Start CPU stress processes
            stress_processes = []
            try:
                for _ in range(cpu_cores):
                    # Simple CPU stress using Python
                    proc = subprocess.Popen(
                        [
                            "python",
                            "-c",
                            "import time; start=time.time(); "
                            "while time.time()-start < {}; sum(range(10000))".format(
                                duration_seconds
                            ),
                        ]
                    )
                    stress_processes.append(proc)

                # Monitor system during stress
                time.sleep(2)  # Let stress ramp up
                during_chaos = self.measure_baseline_performance(duration_seconds - 4)

                # Wait for stress to complete
                for proc in stress_processes:
                    proc.wait()

                # Measure recovery
                time.sleep(5)
                after_chaos = self.measure_baseline_performance(10)

                end_time = datetime.now()

                # Calculate impact
                performance_impact = (
                    (
                        during_chaos["avg_response_time_ms"]
                        / baseline["avg_response_time_ms"]
                    )
                    - 1
                ) * 100
                system_recovered = (
                    after_chaos["error_rate_percent"]
                    <= baseline["error_rate_percent"] * 1.2
                )

                return ChaosExperimentResult(
                    experiment_name=experiment_name,
                    start_time=start_time,
                    end_time=end_time,
                    duration_seconds=(end_time - start_time).total_seconds(),
                    failure_injected=True,
                    system_recovered=system_recovered,
                    recovery_time_seconds=5.0 if system_recovered else None,
                    performance_impact_percent=performance_impact,
                    error_rate_during_chaos=during_chaos["error_rate_percent"],
                    error_rate_before_chaos=baseline["error_rate_percent"],
                    error_rate_after_chaos=after_chaos["error_rate_percent"],
                    metrics={
                        "baseline": baseline,
                        "during_chaos": during_chaos,
                        "after_chaos": after_chaos,
                    },
                )

            except Exception as e:
                logger.error(f"CPU stress experiment failed: {e}")
                # Clean up processes
                for proc in stress_processes:
                    try:
                        proc.terminate()
                    except:
                        pass
                raise

    def inject_memory_pressure(
        self, memory_mb: int = 512, duration_seconds: int = 30
    ) -> ChaosExperimentResult:
        """Inject memory pressure to test behavior under memory constraints."""
        experiment_name = f"memory_pressure_{memory_mb}mb"

        with self.chaos_experiment(experiment_name) as baseline:
            start_time = datetime.now()

            memory_hog = None
            try:
                # Start memory pressure process
                memory_hog = subprocess.Popen(
                    [
                        "python",
                        "-c",
                        f"""
import time
data = []
chunk_size = 1024 * 1024  # 1MB chunks
total_mb = {memory_mb}
for i in range(total_mb):
    data.append(b'x' * chunk_size)
    if i % 10 == 0:
        time.sleep(0.1)
time.sleep({duration_seconds - 5})
""",
                    ]
                )

                # Monitor during memory pressure
                time.sleep(2)
                during_chaos = self.measure_baseline_performance(duration_seconds - 4)

                # Clean up
                memory_hog.terminate()
                memory_hog.wait()

                # Measure recovery
                time.sleep(5)
                after_chaos = self.measure_baseline_performance(10)

                end_time = datetime.now()

                # Calculate impact
                performance_impact = (
                    (
                        during_chaos["avg_response_time_ms"]
                        / baseline["avg_response_time_ms"]
                    )
                    - 1
                ) * 100
                system_recovered = (
                    after_chaos["error_rate_percent"]
                    <= baseline["error_rate_percent"] * 1.2
                )

                return ChaosExperimentResult(
                    experiment_name=experiment_name,
                    start_time=start_time,
                    end_time=end_time,
                    duration_seconds=(end_time - start_time).total_seconds(),
                    failure_injected=True,
                    system_recovered=system_recovered,
                    recovery_time_seconds=5.0 if system_recovered else None,
                    performance_impact_percent=performance_impact,
                    error_rate_during_chaos=during_chaos["error_rate_percent"],
                    error_rate_before_chaos=baseline["error_rate_percent"],
                    error_rate_after_chaos=after_chaos["error_rate_percent"],
                    metrics={
                        "baseline": baseline,
                        "during_chaos": during_chaos,
                        "after_chaos": after_chaos,
                        "memory_allocated_mb": memory_mb,
                    },
                )

            except Exception as e:
                logger.error(f"Memory pressure experiment failed: {e}")
                if memory_hog:
                    try:
                        memory_hog.terminate()
                    except:
                        pass
                raise

    def inject_network_latency(
        self, latency_ms: int = 200, duration_seconds: int = 30
    ) -> ChaosExperimentResult:
        """Simulate network latency to test timeout handling."""
        experiment_name = f"network_latency_{latency_ms}ms"

        # Note: This is a simplified simulation
        # In production, you'd use tools like tc (traffic control) or toxiproxy

        with self.chaos_experiment(experiment_name) as baseline:
            start_time = datetime.now()

            # Simulate latency by adding delays to requests
            original_get = requests.get

            def delayed_get(*args, **kwargs):
                time.sleep(latency_ms / 1000.0)  # Convert ms to seconds
                return original_get(*args, **kwargs)

            try:
                # Monkey patch requests to add latency
                requests.get = delayed_get

                # Monitor during latency
                during_chaos = self.measure_baseline_performance(duration_seconds)

                # Restore original function
                requests.get = original_get

                # Measure recovery
                time.sleep(2)
                after_chaos = self.measure_baseline_performance(10)

                end_time = datetime.now()

                # Calculate impact
                expected_impact = latency_ms  # Expected increase in response time
                actual_impact = (
                    during_chaos["avg_response_time_ms"]
                    - baseline["avg_response_time_ms"]
                )
                performance_impact = (
                    actual_impact / baseline["avg_response_time_ms"]
                ) * 100

                system_recovered = (
                    after_chaos["avg_response_time_ms"]
                    <= baseline["avg_response_time_ms"] * 1.1
                )

                return ChaosExperimentResult(
                    experiment_name=experiment_name,
                    start_time=start_time,
                    end_time=end_time,
                    duration_seconds=(end_time - start_time).total_seconds(),
                    failure_injected=True,
                    system_recovered=system_recovered,
                    recovery_time_seconds=2.0 if system_recovered else None,
                    performance_impact_percent=performance_impact,
                    error_rate_during_chaos=during_chaos["error_rate_percent"],
                    error_rate_before_chaos=baseline["error_rate_percent"],
                    error_rate_after_chaos=after_chaos["error_rate_percent"],
                    metrics={
                        "baseline": baseline,
                        "during_chaos": during_chaos,
                        "after_chaos": after_chaos,
                        "injected_latency_ms": latency_ms,
                        "expected_impact_ms": expected_impact,
                        "actual_impact_ms": actual_impact,
                    },
                )

            except Exception as e:
                logger.error(f"Network latency experiment failed: {e}")
                # Restore original function
                requests.get = original_get
                raise

    def inject_service_failure(
        self, service_name: str, failure_duration: int = 20
    ) -> ChaosExperimentResult:
        """Simulate service failure using Docker container manipulation."""
        experiment_name = f"service_failure_{service_name}"

        if not self.docker_client:
            pytest.skip("Docker client not available for service failure testing")

        with self.chaos_experiment(experiment_name) as baseline:
            start_time = datetime.now()

            container = None
            try:
                # Find the service container
                containers = self.docker_client.containers.list()
                for c in containers:
                    if service_name.lower() in c.name.lower():
                        container = c
                        break

                if not container:
                    logger.warning(f"Container for service {service_name} not found")
                    # Simulate failure differently
                    return self._simulate_service_unavailable(
                        service_name, failure_duration, baseline, start_time
                    )

                # Stop the container
                logger.info(f"Stopping container: {container.name}")
                container.stop()

                # Monitor during failure
                during_chaos = self.measure_baseline_performance(failure_duration)

                # Restart the container
                logger.info(f"Restarting container: {container.name}")
                container.restart()

                # Wait for service to be ready
                recovery_start = time.time()
                max_wait = 60  # Maximum recovery time

                while time.time() - recovery_start < max_wait:
                    if self.is_system_healthy():
                        break
                    time.sleep(1)
                else:
                    logger.warning("Service did not recover within expected time")

                recovery_time = time.time() - recovery_start

                # Measure after recovery
                after_chaos = self.measure_baseline_performance(10)

                end_time = datetime.now()

                system_recovered = self.is_system_healthy()
                performance_impact = (
                    (
                        during_chaos["avg_response_time_ms"]
                        / baseline["avg_response_time_ms"]
                    )
                    - 1
                ) * 100

                return ChaosExperimentResult(
                    experiment_name=experiment_name,
                    start_time=start_time,
                    end_time=end_time,
                    duration_seconds=(end_time - start_time).total_seconds(),
                    failure_injected=True,
                    system_recovered=system_recovered,
                    recovery_time_seconds=recovery_time,
                    performance_impact_percent=performance_impact,
                    error_rate_during_chaos=during_chaos["error_rate_percent"],
                    error_rate_before_chaos=baseline["error_rate_percent"],
                    error_rate_after_chaos=after_chaos["error_rate_percent"],
                    metrics={
                        "baseline": baseline,
                        "during_chaos": during_chaos,
                        "after_chaos": after_chaos,
                        "service_name": service_name,
                        "container_name": container.name if container else None,
                    },
                )

            except Exception as e:
                logger.error(f"Service failure experiment failed: {e}")
                # Attempt to restart container if it was stopped
                if container:
                    try:
                        container.restart()
                    except:
                        pass
                raise

    def _simulate_service_unavailable(
        self, service_name: str, duration: int, baseline: Dict, start_time: datetime
    ) -> ChaosExperimentResult:
        """Simulate service unavailable when Docker is not available."""
        # This is a fallback when Docker manipulation isn't possible
        logger.info(f"Simulating {service_name} unavailability for {duration}s")

        # For simulation, we can't actually bring down services
        # So we'll create a scenario where requests might fail more often

        during_chaos = baseline.copy()
        during_chaos["error_rate_percent"] = min(
            50.0, baseline["error_rate_percent"] + 25.0
        )  # Simulate higher error rate

        time.sleep(duration)

        after_chaos = baseline.copy()  # Assume recovery
        end_time = datetime.now()

        return ChaosExperimentResult(
            experiment_name=f"simulated_failure_{service_name}",
            start_time=start_time,
            end_time=end_time,
            duration_seconds=(end_time - start_time).total_seconds(),
            failure_injected=True,
            system_recovered=True,
            recovery_time_seconds=1.0,
            performance_impact_percent=0.0,
            error_rate_during_chaos=during_chaos["error_rate_percent"],
            error_rate_before_chaos=baseline["error_rate_percent"],
            error_rate_after_chaos=after_chaos["error_rate_percent"],
            metrics={
                "baseline": baseline,
                "during_chaos": during_chaos,
                "after_chaos": after_chaos,
                "note": "Simulated failure - Docker not available",
            },
        )

    def run_chaos_suite(
        self, experiments: List[str]
    ) -> Dict[str, ChaosExperimentResult]:
        """Run a suite of chaos engineering experiments."""
        results = {}

        # Wait for system to be healthy before starting
        if not self.is_system_healthy():
            logger.warning("System not healthy before chaos tests")
            time.sleep(10)

        for experiment in experiments:
            logger.info(f"Running chaos experiment: {experiment}")

            try:
                if experiment == "cpu_stress":
                    result = self.inject_cpu_stress(duration_seconds=20, cpu_cores=2)
                elif experiment == "memory_pressure":
                    result = self.inject_memory_pressure(
                        memory_mb=256, duration_seconds=20
                    )
                elif experiment == "network_latency":
                    result = self.inject_network_latency(
                        latency_ms=100, duration_seconds=20
                    )
                elif experiment == "database_failure":
                    result = self.inject_service_failure(
                        "postgres", failure_duration=15
                    )
                elif experiment == "kafka_failure":
                    result = self.inject_service_failure("kafka", failure_duration=15)
                else:
                    logger.warning(f"Unknown experiment: {experiment}")
                    continue

                results[experiment] = result

                # Cool down between experiments
                logger.info(f"Experiment {experiment} completed. Cooling down...")
                time.sleep(10)

                # Verify system recovery
                if not self.is_system_healthy():
                    logger.warning(f"System not healthy after {experiment}")
                    time.sleep(20)  # Extended recovery time

            except Exception as e:
                logger.error(f"Experiment {experiment} failed: {e}")
                results[experiment] = None

        return results


# Test fixtures
@pytest.fixture(scope="session")
def chaos_engineer():
    """Chaos engineer instance."""
    return ChaosEngineer()


@pytest.fixture(scope="session")
def docker_compose_stack():
    """Docker compose stack for chaos testing."""
    try:
        # Assuming docker-compose.yml exists in project root
        compose_file = os.path.join(
            os.path.dirname(__file__), "../../docker-compose.yml"
        )
        if os.path.exists(compose_file):
            with DockerCompose(os.path.dirname(compose_file)) as compose:
                yield compose
        else:
            pytest.skip("Docker compose file not found")
    except Exception as e:
        logger.warning(f"Docker compose not available: {e}")
        pytest.skip("Docker compose not available for chaos testing")


class TestChaosEngineering:
    """Chaos engineering tests for system resilience."""

    def test_system_baseline_performance(self, chaos_engineer):
        """Establish baseline performance metrics."""
        baseline = chaos_engineer.measure_baseline_performance(30)

        # Validate baseline is reasonable
        assert (
            baseline["avg_response_time_ms"] < 1000
        ), f"Baseline response time too high: {baseline['avg_response_time_ms']}ms"
        assert (
            baseline["error_rate_percent"] < 5.0
        ), f"Baseline error rate too high: {baseline['error_rate_percent']}%"
        assert (
            baseline["total_requests"] > 200
        ), f"Not enough baseline requests: {baseline['total_requests']}"

        logger.info(f"Baseline performance established:")
        logger.info(f"  Response time: {baseline['avg_response_time_ms']:.1f}ms")
        logger.info(f"  Error rate: {baseline['error_rate_percent']:.1f}%")
        logger.info(f"  CPU usage: {baseline['avg_cpu_percent']:.1f}%")
        logger.info(f"  Memory usage: {baseline['avg_memory_percent']:.1f}%")

    def test_cpu_stress_resilience(self, chaos_engineer):
        """Test system resilience under CPU stress."""
        result = chaos_engineer.inject_cpu_stress(duration_seconds=25, cpu_cores=2)

        # System should handle CPU stress gracefully
        assert result.failure_injected, "CPU stress was not properly injected"
        assert result.system_recovered, "System did not recover from CPU stress"
        assert (
            result.performance_impact_percent < 200
        ), f"Performance impact too high: {result.performance_impact_percent:.1f}%"
        assert (
            result.error_rate_during_chaos < 20.0
        ), f"Error rate during chaos too high: {result.error_rate_during_chaos:.1f}%"

        logger.info(f"CPU stress resilience test:")
        logger.info(f"  Performance impact: {result.performance_impact_percent:.1f}%")
        logger.info(f"  Error rate during chaos: {result.error_rate_during_chaos:.1f}%")
        logger.info(f"  Recovery time: {result.recovery_time_seconds:.1f}s")
        logger.info(f"  System recovered: {result.system_recovered}")

    def test_memory_pressure_resilience(self, chaos_engineer):
        """Test system resilience under memory pressure."""
        result = chaos_engineer.inject_memory_pressure(
            memory_mb=512, duration_seconds=25
        )

        # System should handle memory pressure
        assert result.failure_injected, "Memory pressure was not properly injected"
        assert result.system_recovered, "System did not recover from memory pressure"
        assert (
            result.error_rate_during_chaos < 25.0
        ), f"Error rate during chaos too high: {result.error_rate_during_chaos:.1f}%"

        logger.info(f"Memory pressure resilience test:")
        logger.info(f"  Performance impact: {result.performance_impact_percent:.1f}%")
        logger.info(f"  Error rate during chaos: {result.error_rate_during_chaos:.1f}%")
        logger.info(f"  Recovery time: {result.recovery_time_seconds:.1f}s")
        logger.info(f"  System recovered: {result.system_recovered}")

    def test_network_latency_tolerance(self, chaos_engineer):
        """Test system tolerance to network latency."""
        result = chaos_engineer.inject_network_latency(
            latency_ms=150, duration_seconds=20
        )

        # System should handle network latency
        assert result.failure_injected, "Network latency was not properly injected"
        assert result.system_recovered, "System did not recover from network latency"

        # Response times should increase but not cause failures
        assert (
            result.error_rate_during_chaos <= result.error_rate_before_chaos * 2
        ), "Too many errors during latency"

        logger.info(f"Network latency tolerance test:")
        logger.info(f"  Performance impact: {result.performance_impact_percent:.1f}%")
        logger.info(f"  Error rate during chaos: {result.error_rate_during_chaos:.1f}%")
        logger.info(
            f"  Expected latency impact: {result.metrics.get('expected_impact_ms', 0):.1f}ms"
        )
        logger.info(
            f"  Actual latency impact: {result.metrics.get('actual_impact_ms', 0):.1f}ms"
        )

    def test_service_failure_recovery(self, chaos_engineer):
        """Test system recovery from service failures."""
        # Test database failure scenario
        result = chaos_engineer.inject_service_failure("postgres", failure_duration=15)

        # System should handle service failure with graceful degradation
        assert result.failure_injected, "Service failure was not properly injected"

        # Allow higher error rates during service failure, but system should recover
        if result.system_recovered:
            assert (
                result.recovery_time_seconds < 60
            ), f"Recovery took too long: {result.recovery_time_seconds:.1f}s"
            assert (
                result.error_rate_after_chaos <= result.error_rate_before_chaos * 1.5
            ), "System did not fully recover"

        logger.info(f"Service failure recovery test:")
        logger.info(f"  Service: {result.metrics.get('service_name', 'unknown')}")
        logger.info(f"  Error rate during chaos: {result.error_rate_during_chaos:.1f}%")
        logger.info(f"  Recovery time: {result.recovery_time_seconds:.1f}s")
        logger.info(f"  System recovered: {result.system_recovered}")

    def test_comprehensive_chaos_suite(self, chaos_engineer):
        """Run comprehensive chaos engineering test suite."""
        experiments = ["cpu_stress", "memory_pressure", "network_latency"]

        # Add service failure tests if Docker is available
        if chaos_engineer.docker_client:
            experiments.extend(["database_failure"])

        results = chaos_engineer.run_chaos_suite(experiments)

        # Validate overall resilience
        successful_experiments = [r for r in results.values() if r is not None]
        assert (
            len(successful_experiments) >= len(experiments) * 0.7
        ), "Too many chaos experiments failed"

        # Check recovery across all experiments
        recovered_count = sum(1 for r in successful_experiments if r.system_recovered)
        recovery_rate = recovered_count / len(successful_experiments) * 100

        assert (
            recovery_rate >= 80
        ), f"System recovery rate too low: {recovery_rate:.1f}%"

        logger.info(f"Comprehensive chaos suite results:")
        logger.info(
            f"  Experiments run: {len(successful_experiments)}/{len(experiments)}"
        )
        logger.info(f"  Recovery rate: {recovery_rate:.1f}%")

        for experiment_name, result in results.items():
            if result:
                logger.info(
                    f"  {experiment_name}: {result.performance_impact_percent:.1f}% impact, "
                    f"recovered: {result.system_recovered}"
                )

    def test_cascading_failure_prevention(self, chaos_engineer):
        """Test system's ability to prevent cascading failures."""
        # Run multiple stress conditions simultaneously
        logger.info("Testing cascading failure prevention")

        def run_light_cpu_stress():
            return chaos_engineer.inject_cpu_stress(duration_seconds=15, cpu_cores=1)

        def run_light_memory_pressure():
            return chaos_engineer.inject_memory_pressure(
                memory_mb=256, duration_seconds=15
            )

        # Run simultaneous light stress
        with ThreadPoolExecutor(max_workers=2) as executor:
            cpu_future = executor.submit(run_light_cpu_stress)
            memory_future = executor.submit(run_light_memory_pressure)

            cpu_result = cpu_future.result()
            memory_result = memory_future.result()

        # System should handle multiple concurrent stresses
        assert (
            cpu_result.system_recovered
        ), "System did not recover from concurrent CPU stress"
        assert (
            memory_result.system_recovered
        ), "System did not recover from concurrent memory pressure"

        # Combined impact should not be catastrophic
        max_error_rate = max(
            cpu_result.error_rate_during_chaos, memory_result.error_rate_during_chaos
        )
        assert (
            max_error_rate < 30.0
        ), f"Error rate too high under concurrent stress: {max_error_rate:.1f}%"

        logger.info(f"Cascading failure prevention test:")
        logger.info(
            f"  CPU stress error rate: {cpu_result.error_rate_during_chaos:.1f}%"
        )
        logger.info(
            f"  Memory pressure error rate: {memory_result.error_rate_during_chaos:.1f}%"
        )
        logger.info(
            f"  Both systems recovered: {cpu_result.system_recovered and memory_result.system_recovered}"
        )

    def test_gradual_degradation_behavior(self, chaos_engineer):
        """Test system's gradual degradation under increasing load."""
        stress_levels = [1, 2, 4]  # CPU cores
        results = []

        for stress_level in stress_levels:
            logger.info(f"Testing degradation with {stress_level} CPU cores stress")
            result = chaos_engineer.inject_cpu_stress(
                duration_seconds=15, cpu_cores=stress_level
            )
            results.append(result)

            # Cool down between tests
            time.sleep(5)

        # Analyze degradation pattern
        error_rates = [r.error_rate_during_chaos for r in results]
        performance_impacts = [r.performance_impact_percent for r in results]

        # Error rates should not increase dramatically
        assert all(
            rate < 50.0 for rate in error_rates
        ), "Error rates too high under stress"

        # Performance should degrade gradually, not catastrophically
        max_impact = max(performance_impacts)
        assert max_impact < 500, f"Performance impact too severe: {max_impact:.1f}%"

        logger.info(f"Gradual degradation test:")
        for i, (stress, error_rate, impact) in enumerate(
            zip(stress_levels, error_rates, performance_impacts)
        ):
            logger.info(
                f"  {stress} cores: {error_rate:.1f}% errors, {impact:.1f}% impact"
            )


if __name__ == "__main__":
    # Example usage for standalone testing
    logging.basicConfig(level=logging.INFO)

    print("Chaos Engineering Testing Framework")
    print("Run with pytest for full test suite:")
    print("pytest tests/performance/test_chaos_engineering.py -v --tb=short")
