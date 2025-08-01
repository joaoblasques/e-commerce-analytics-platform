#!/usr/bin/env python3
"""
E-Commerce Analytics Platform - Comprehensive Health Check Script
Performs comprehensive health checks on all services in the platform.
"""

import argparse
import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List

# Import existing health check modules
try:
    import importlib.util
    import os

    # Import test-services.py
    spec_services = importlib.util.spec_from_file_location(
        "test_services", os.path.join(os.path.dirname(__file__), "test-services.py")
    )
    test_services = importlib.util.module_from_spec(spec_services)
    spec_services.loader.exec_module(test_services)

    # Import test-monitoring.py
    spec_monitoring = importlib.util.spec_from_file_location(
        "test_monitoring", os.path.join(os.path.dirname(__file__), "test-monitoring.py")
    )
    test_monitoring = importlib.util.module_from_spec(spec_monitoring)
    spec_monitoring.loader.exec_module(test_monitoring)

except Exception as e:
    print(f"❌ Failed to import health check modules: {e}")
    print("Make sure test-services.py and test-monitoring.py are in the same directory")
    sys.exit(1)


class ComprehensiveHealthChecker:
    """Comprehensive health checker for all platform services."""

    def __init__(self, include_monitoring: bool = True, include_docker: bool = True):
        self.include_monitoring = include_monitoring
        self.include_docker = include_docker
        self.results = {}
        self.start_time = None
        self.end_time = None

    def check_docker_status(self) -> Dict[str, Any]:
        """Check Docker and Docker Compose status."""
        if not self.include_docker:
            return {"status": "skipped", "message": "Docker check disabled"}

        try:
            # Check Docker daemon
            result = subprocess.run(["docker", "info"], capture_output=True, text=True)
            if result.returncode != 0:
                return {"status": "unhealthy", "error": "Docker daemon not running"}

            # Check Docker Compose
            result = subprocess.run(
                ["docker-compose", "--version"], capture_output=True, text=True
            )
            if result.returncode != 0:
                return {"status": "unhealthy", "error": "Docker Compose not available"}

            # Check running containers
            result = subprocess.run(
                ["docker-compose", "ps", "-q"], capture_output=True, text=True
            )
            if result.returncode != 0:
                return {"status": "unhealthy", "error": "Cannot check container status"}

            running_containers = (
                len(result.stdout.strip().split("\n")) if result.stdout.strip() else 0
            )

            return {
                "status": "healthy",
                "running_containers": running_containers,
                "docker_version": subprocess.run(
                    ["docker", "--version"], capture_output=True, text=True
                ).stdout.strip(),
                "compose_version": subprocess.run(
                    ["docker-compose", "--version"], capture_output=True, text=True
                ).stdout.strip(),
            }

        except Exception as e:
            return {"status": "error", "error": str(e)}

    def check_core_services(self) -> Dict[str, Any]:
        """Check core services (PostgreSQL, Redis, MinIO, Spark, Kafka)."""
        try:
            # Use existing test_services functions
            tests = [
                ("postgres", test_services.test_postgres),
                ("redis", test_services.test_redis),
                ("minio", test_services.test_minio),
                ("spark", test_services.test_spark),
                ("kafka", test_services.test_kafka),
            ]

            results = {}
            for service_name, test_func in tests:
                try:
                    results[service_name] = test_func()
                except Exception as e:
                    results[service_name] = {"status": "error", "error": str(e)}

            return results

        except Exception as e:
            return {"status": "error", "error": str(e)}

    def check_monitoring_services(self) -> Dict[str, Any]:
        """Check monitoring services (Prometheus, Grafana, Alertmanager, etc.)."""
        if not self.include_monitoring:
            return {"status": "skipped", "message": "Monitoring check disabled"}

        try:
            # Use existing test_monitoring functions
            results = []

            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_service = {
                    executor.submit(test_monitoring.test_service, service): service
                    for service in test_monitoring.MONITORING_SERVICES
                }

                for future in as_completed(future_to_service):
                    result = future.result()
                    results.append(result)

            # Convert to dictionary format
            monitoring_results = {}
            for result in results:
                service_name = result["name"].lower().replace(" ", "_")
                monitoring_results[service_name] = result

            return monitoring_results

        except Exception as e:
            return {"status": "error", "error": str(e)}

    def check_network_connectivity(self) -> Dict[str, Any]:
        """Check network connectivity between services."""
        try:
            # Check if services can communicate with each other
            connectivity_tests = [
                ("postgres_to_redis", "localhost", 6379),
                ("postgres_to_kafka", "localhost", 9092),
                ("redis_to_minio", "localhost", 9000),
                ("spark_to_kafka", "localhost", 9092),
                ("prometheus_to_grafana", "localhost", 3000),
            ]

            results = {}
            for test_name, host, port in connectivity_tests:
                try:
                    import socket

                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5)
                    result = sock.connect_ex((host, port))
                    sock.close()

                    results[test_name] = {
                        "status": "healthy" if result == 0 else "unhealthy",
                        "host": host,
                        "port": port,
                        "result_code": result,
                    }
                except Exception as e:
                    results[test_name] = {"status": "error", "error": str(e)}

            return results

        except Exception as e:
            return {"status": "error", "error": str(e)}

    def check_disk_space(self) -> Dict[str, Any]:
        """Check disk space usage."""
        try:
            import shutil

            # Check disk space for key directories
            directories = [("project_root", "."), ("logs", "logs"), ("tmp", "/tmp")]

            results = {}
            for name, path in directories:
                try:
                    total, used, free = shutil.disk_usage(path)
                    usage_percent = (used / total) * 100

                    results[name] = {
                        "status": "healthy"
                        if usage_percent < 90
                        else "warning"
                        if usage_percent < 95
                        else "critical",
                        "total_gb": round(total / (1024**3), 2),
                        "used_gb": round(used / (1024**3), 2),
                        "free_gb": round(free / (1024**3), 2),
                        "usage_percent": round(usage_percent, 1),
                    }
                except Exception as e:
                    results[name] = {"status": "error", "error": str(e)}

            return results

        except Exception as e:
            return {"status": "error", "error": str(e)}

    def check_memory_usage(self) -> Dict[str, Any]:
        """Check system memory usage."""
        try:
            import psutil

            # Get memory info
            memory = psutil.virtual_memory()
            swap = psutil.swap_memory()

            return {
                "virtual_memory": {
                    "status": "healthy"
                    if memory.percent < 85
                    else "warning"
                    if memory.percent < 95
                    else "critical",
                    "total_gb": round(memory.total / (1024**3), 2),
                    "used_gb": round(memory.used / (1024**3), 2),
                    "free_gb": round(memory.free / (1024**3), 2),
                    "usage_percent": round(memory.percent, 1),
                },
                "swap_memory": {
                    "status": "healthy"
                    if swap.percent < 50
                    else "warning"
                    if swap.percent < 80
                    else "critical",
                    "total_gb": round(swap.total / (1024**3), 2),
                    "used_gb": round(swap.used / (1024**3), 2),
                    "free_gb": round(swap.free / (1024**3), 2),
                    "usage_percent": round(swap.percent, 1),
                },
            }

        except ImportError:
            return {"status": "skipped", "message": "psutil not available"}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def run_comprehensive_check(self) -> Dict[str, Any]:
        """Run all health checks."""
        self.start_time = datetime.now()

        print("🔍 Running comprehensive health check...")
        print("=" * 60)

        # Run all checks
        checks = [
            ("docker", self.check_docker_status),
            ("core_services", self.check_core_services),
            ("monitoring_services", self.check_monitoring_services),
            ("network_connectivity", self.check_network_connectivity),
            ("disk_space", self.check_disk_space),
            ("memory_usage", self.check_memory_usage),
        ]

        self.results = {}
        for check_name, check_func in checks:
            print(f"Checking {check_name}...", end=" ")
            try:
                self.results[check_name] = check_func()
                print("✅")
            except Exception as e:
                self.results[check_name] = {"status": "error", "error": str(e)}
                print("❌")

        self.end_time = datetime.now()
        duration = (self.end_time - self.start_time).total_seconds()

        self.results["_metadata"] = {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "duration_seconds": round(duration, 2),
            "total_checks": len(checks),
        }

        return self.results

    def print_detailed_report(self):
        """Print detailed health check report."""
        print("\n" + "=" * 60)
        print("COMPREHENSIVE HEALTH CHECK REPORT")
        print("=" * 60)

        overall_status = "HEALTHY"
        critical_issues = []
        warnings = []

        for category, result in self.results.items():
            if category == "_metadata":
                continue

            print(f"\n📊 {category.upper().replace('_', ' ')}")
            print("-" * 40)

            if isinstance(result, dict):
                if "status" in result:
                    # Single service result
                    status = result["status"]
                    if status in ["unhealthy", "critical", "error"]:
                        overall_status = "UNHEALTHY"
                        critical_issues.append(
                            f"{category}: {result.get('error', 'Unknown issue')}"
                        )
                    elif status == "warning":
                        warnings.append(f"{category}: Warning condition detected")

                    status_icon = self._get_status_icon(status)
                    print(f"  {status_icon} Status: {status.upper()}")

                    if "error" in result:
                        print(f"  ❌ Error: {result['error']}")

                else:
                    # Multiple service results
                    for service, service_result in result.items():
                        if (
                            isinstance(service_result, dict)
                            and "status" in service_result
                        ):
                            status = service_result["status"]
                            if status in ["unhealthy", "critical", "error"]:
                                overall_status = "UNHEALTHY"
                                critical_issues.append(
                                    f"{category}.{service}: {service_result.get('error', 'Unknown issue')}"
                                )
                            elif status == "warning":
                                warnings.append(
                                    f"{category}.{service}: Warning condition detected"
                                )

                            status_icon = self._get_status_icon(status)
                            print(f"  {status_icon} {service}: {status.upper()}")

                            if "error" in service_result:
                                print(f"    ❌ Error: {service_result['error']}")

        # Print summary
        print(f"\n{'=' * 60}")
        print("SUMMARY")
        print("=" * 60)

        metadata = self.results.get("_metadata", {})
        print(f"Start Time: {metadata.get('start_time', 'Unknown')}")
        print(f"Duration: {metadata.get('duration_seconds', 0)} seconds")
        print(f"Total Checks: {metadata.get('total_checks', 0)}")

        if overall_status == "HEALTHY" and not warnings:
            print("\n🎉 ALL SYSTEMS HEALTHY!")
            print("The e-commerce analytics platform is ready for use.")
        else:
            print(f"\n🚨 OVERALL STATUS: {overall_status}")

            if critical_issues:
                print(f"\n❌ CRITICAL ISSUES ({len(critical_issues)}):")
                for issue in critical_issues:
                    print(f"  - {issue}")

            if warnings:
                print(f"\n⚠️ WARNINGS ({len(warnings)}):")
                for warning in warnings:
                    print(f"  - {warning}")

        print(f"\n{'=' * 60}")

    def _get_status_icon(self, status: str) -> str:
        """Get status icon for display."""
        icons = {
            "healthy": "✅",
            "unhealthy": "❌",
            "warning": "⚠️",
            "critical": "🚨",
            "error": "❌",
            "skipped": "⏭️",
            "partial": "⚠️",
        }
        return icons.get(status, "❓")

    def save_report(self, filename: str = None):
        """Save health check report to file."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"health_check_report_{timestamp}.json"

        try:
            with open(filename, "w") as f:
                json.dump(self.results, f, indent=2, default=str)
            print(f"📄 Health check report saved to: {filename}")
        except Exception as e:
            print(f"❌ Failed to save report: {e}")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Comprehensive health check for e-commerce analytics platform"
    )
    parser.add_argument(
        "--no-monitoring", action="store_true", help="Skip monitoring services check"
    )
    parser.add_argument(
        "--no-docker", action="store_true", help="Skip Docker status check"
    )
    parser.add_argument("--save-report", type=str, help="Save report to specified file")
    parser.add_argument(
        "--json-only", action="store_true", help="Output only JSON results"
    )

    args = parser.parse_args()

    # Create health checker
    checker = ComprehensiveHealthChecker(
        include_monitoring=not args.no_monitoring, include_docker=not args.no_docker
    )

    # Run checks
    results = checker.run_comprehensive_check()

    if args.json_only:
        print(json.dumps(results, indent=2, default=str))
    else:
        checker.print_detailed_report()

    # Save report if requested
    if args.save_report:
        checker.save_report(args.save_report)

    # Exit with appropriate code
    overall_healthy = True
    for category, result in results.items():
        if category == "_metadata":
            continue

        if isinstance(result, dict):
            if "status" in result and result["status"] in [
                "unhealthy",
                "critical",
                "error",
            ]:
                overall_healthy = False
                break
            else:
                for service_result in result.values():
                    if isinstance(service_result, dict) and "status" in service_result:
                        if service_result["status"] in [
                            "unhealthy",
                            "critical",
                            "error",
                        ]:
                            overall_healthy = False
                            break

    sys.exit(0 if overall_healthy else 1)


if __name__ == "__main__":
    main()
