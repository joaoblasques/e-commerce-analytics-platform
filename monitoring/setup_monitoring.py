#!/usr/bin/env python3
"""
ECAP Monitoring Setup Script.

This script sets up and configures the complete monitoring stack including:
- Prometheus for metrics collection
- Grafana for visualization
- Jaeger for distributed tracing
- AlertManager for alerting
- Various exporters for comprehensive monitoring

Usage:
    python monitoring/setup_monitoring.py [--environment dev|staging|prod]
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

import requests


class MonitoringSetup:
    """ECAP Monitoring Setup Manager."""

    def __init__(self, environment: str = "development"):
        self.environment = environment
        self.base_dir = Path(__file__).parent
        self.project_root = self.base_dir.parent

        # Service URLs
        self.services = {
            "prometheus": "http://localhost:9090",
            "grafana": "http://localhost:3001",
            "jaeger": "http://localhost:16686",
            "alertmanager": "http://localhost:9093",
        }

        # Grafana credentials
        self.grafana_auth = ("admin", "admin123")

    def setup_complete_monitoring(self):
        """Set up the complete monitoring stack."""
        print("🚀 Setting up ECAP Application Performance Monitoring...")

        try:
            # Step 1: Start monitoring stack
            self._start_monitoring_stack()

            # Step 2: Wait for services to be ready
            self._wait_for_services()

            # Step 3: Configure Grafana
            self._configure_grafana()

            # Step 4: Validate setup
            self._validate_setup()

            # Step 5: Display access information
            self._display_access_info()

            print("✅ Monitoring setup completed successfully!")

        except Exception as e:
            print(f"❌ Setup failed: {e}")
            sys.exit(1)

    def _start_monitoring_stack(self):
        """Start the monitoring Docker stack."""
        print("📦 Starting monitoring Docker containers...")

        compose_file = self.base_dir / "docker-compose.monitoring.yml"

        cmd = ["docker-compose", "-f", str(compose_file), "up", "-d"]

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            raise Exception(f"Failed to start monitoring stack: {result.stderr}")

        print("✅ Monitoring containers started")

    def _wait_for_services(self):
        """Wait for all monitoring services to be ready."""
        print("⏳ Waiting for services to be ready...")

        for service_name, url in self.services.items():
            print(f"  Waiting for {service_name}...")
            self._wait_for_service(url, service_name)
            print(f"  ✅ {service_name} is ready")

        print("✅ All services are ready")

    def _wait_for_service(self, url: str, service_name: str, timeout: int = 120):
        """Wait for a specific service to be ready."""
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                if service_name == "grafana":
                    response = requests.get(f"{url}/api/health", timeout=5)
                elif service_name == "prometheus":
                    response = requests.get(f"{url}/-/healthy", timeout=5)
                elif service_name == "jaeger":
                    response = requests.get(f"{url}/", timeout=5)
                elif service_name == "alertmanager":
                    response = requests.get(f"{url}/-/healthy", timeout=5)
                else:
                    response = requests.get(url, timeout=5)

                if response.status_code == 200:
                    return

            except requests.exceptions.RequestException:
                pass

            time.sleep(2)

        raise Exception(
            f"Service {service_name} did not become ready within {timeout} seconds"
        )

    def _configure_grafana(self):
        """Configure Grafana with datasources and dashboards."""
        print("🎨 Configuring Grafana...")

        # Create API client
        grafana_url = self.services["grafana"]

        # Import dashboards
        self._import_grafana_dashboards()

        print("✅ Grafana configured successfully")

    def _import_grafana_dashboards(self):
        """Import pre-configured dashboards into Grafana."""
        dashboard_dir = self.base_dir / "grafana" / "dashboards"

        if not dashboard_dir.exists():
            print("⚠️ Dashboard directory not found, skipping dashboard import")
            return

        headers = {"Content-Type": "application/json"}

        for dashboard_file in dashboard_dir.glob("*.json"):
            print(f"  Importing dashboard: {dashboard_file.name}")

            try:
                with open(dashboard_file, "r") as f:
                    dashboard_data = json.load(f)

                # Wrap dashboard in import format
                import_data = {
                    "dashboard": dashboard_data.get("dashboard", dashboard_data),
                    "overwrite": True,
                    "inputs": [],
                    "folderId": 0,
                }

                response = requests.post(
                    f"{self.services['grafana']}/api/dashboards/db",
                    json=import_data,
                    headers=headers,
                    auth=self.grafana_auth,
                    timeout=10,
                )

                if response.status_code in [200, 201]:
                    print(
                        f"    ✅ Dashboard {dashboard_file.name} imported successfully"
                    )
                else:
                    print(
                        f"    ⚠️ Failed to import {dashboard_file.name}: {response.text}"
                    )

            except Exception as e:
                print(f"    ❌ Error importing {dashboard_file.name}: {e}")

    def _validate_setup(self):
        """Validate that the monitoring setup is working correctly."""
        print("🔍 Validating monitoring setup...")

        validations = [
            ("Prometheus targets", self._validate_prometheus_targets),
            ("Grafana datasources", self._validate_grafana_datasources),
            ("AlertManager config", self._validate_alertmanager_config),
            ("Jaeger readiness", self._validate_jaeger_readiness),
        ]

        for description, validator in validations:
            try:
                validator()
                print(f"  ✅ {description}")
            except Exception as e:
                print(f"  ⚠️ {description} validation failed: {e}")

    def _validate_prometheus_targets(self):
        """Validate Prometheus targets are up."""
        response = requests.get(f"{self.services['prometheus']}/api/v1/targets")
        response.raise_for_status()

        targets = response.json()["data"]["activeTargets"]
        up_targets = [t for t in targets if t["health"] == "up"]

        if len(up_targets) == 0:
            raise Exception("No Prometheus targets are up")

    def _validate_grafana_datasources(self):
        """Validate Grafana datasources are working."""
        response = requests.get(
            f"{self.services['grafana']}/api/datasources", auth=self.grafana_auth
        )
        response.raise_for_status()

        datasources = response.json()
        if len(datasources) == 0:
            raise Exception("No Grafana datasources configured")

    def _validate_alertmanager_config(self):
        """Validate AlertManager configuration."""
        response = requests.get(f"{self.services['alertmanager']}/api/v1/status")
        response.raise_for_status()

    def _validate_jaeger_readiness(self):
        """Validate Jaeger is ready."""
        response = requests.get(f"{self.services['jaeger']}/")
        response.raise_for_status()

    def _display_access_info(self):
        """Display access information for all services."""
        print("\n" + "=" * 60)
        print("🎯 ECAP Monitoring Stack Access Information")
        print("=" * 60)

        services_info = [
            ("📊 Grafana (Dashboards)", self.services["grafana"], "admin / admin123"),
            ("📈 Prometheus (Metrics)", self.services["prometheus"], "No auth"),
            ("🔍 Jaeger (Tracing)", self.services["jaeger"], "No auth"),
            ("🚨 AlertManager (Alerts)", self.services["alertmanager"], "No auth"),
        ]

        for name, url, auth in services_info:
            print(f"{name}")
            print(f"  URL: {url}")
            print(f"  Auth: {auth}")
            print()

        print("📋 Available Dashboards:")
        print("  • ECAP - Application Performance Overview")
        print("  • ECAP - SLA Monitoring Dashboard")
        print()

        print("🔧 Key Metrics Being Monitored:")
        print("  • API Request Rate & Response Times")
        print("  • Error Rates & Availability SLA")
        print("  • Database & Redis Performance")
        print("  • Kafka Message Processing")
        print("  • System Resources (CPU, Memory, Disk)")
        print("  • Custom Business Metrics")
        print()

        print("⚡ To start collecting metrics from your application:")
        print("  1. Start your ECAP services (docker-compose up)")
        print("  2. Visit http://localhost:8000/metrics to see Prometheus metrics")
        print("  3. Generate some traffic to see data in dashboards")
        print()

    def stop_monitoring(self):
        """Stop the monitoring stack."""
        print("🛑 Stopping monitoring stack...")

        compose_file = self.base_dir / "docker-compose.monitoring.yml"

        cmd = ["docker-compose", "-f", str(compose_file), "down"]

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            print(f"⚠️ Warning: {result.stderr}")
        else:
            print("✅ Monitoring stack stopped")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="ECAP Monitoring Setup")
    parser.add_argument(
        "--environment",
        choices=["dev", "staging", "prod"],
        default="dev",
        help="Deployment environment",
    )
    parser.add_argument("--stop", action="store_true", help="Stop the monitoring stack")

    args = parser.parse_args()

    setup = MonitoringSetup(environment=args.environment)

    if args.stop:
        setup.stop_monitoring()
    else:
        setup.setup_complete_monitoring()


if __name__ == "__main__":
    main()
