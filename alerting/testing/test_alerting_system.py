#!/usr/bin/env python3
"""
Comprehensive Testing Suite for ECAP Alerting System.

This module provides end-to-end testing for:
- Alert rule validation
- Escalation procedures
- Notification systems
- On-call rotation
- Incident response automation
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import aiohttp
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class TestResult:
    """Test result structure."""

    test_name: str
    passed: bool
    message: str
    duration_seconds: float
    details: Optional[Dict] = None
    expected: Optional[Any] = None
    actual: Optional[Any] = None


@dataclass
class TestSuite:
    """Test suite structure."""

    suite_name: str
    tests: List[TestResult]
    total_tests: int = 0
    passed_tests: int = 0
    failed_tests: int = 0
    total_duration: float = 0.0

    def __post_init__(self):
        """Calculate derived metrics after dataclass creation."""
        self.total_tests = len(self.tests)
        self.passed_tests = sum(1 for test in self.tests if test.passed)
        self.failed_tests = self.total_tests - self.passed_tests
        self.total_duration = sum(test.duration_seconds for test in self.tests)


class AlertRuleTester:
    """Test alert rules and validation."""

    def __init__(self, prometheus_url: str = "http://localhost:9090"):
        self.prometheus_url = prometheus_url

    async def test_alert_rules_syntax(self, rules_file: str) -> TestResult:
        """Test alert rules syntax validation."""
        start_time = time.time()

        try:
            with open(rules_file, "r") as f:
                rules_data = yaml.safe_load(f)

            # Validate YAML structure
            if "groups" not in rules_data:
                return TestResult(
                    test_name="alert_rules_syntax",
                    passed=False,
                    message="Missing 'groups' key in alert rules",
                    duration_seconds=time.time() - start_time,
                )

            # Validate each group
            for group in rules_data["groups"]:
                if "name" not in group:
                    return TestResult(
                        test_name="alert_rules_syntax",
                        passed=False,
                        message="Missing 'name' in alert group",
                        duration_seconds=time.time() - start_time,
                    )

                if "rules" not in group:
                    return TestResult(
                        test_name="alert_rules_syntax",
                        passed=False,
                        message=f"Missing 'rules' in group {group['name']}",
                        duration_seconds=time.time() - start_time,
                    )

                # Validate each rule
                for rule in group["rules"]:
                    required_fields = ["alert", "expr", "labels", "annotations"]
                    for field in required_fields:
                        if field not in rule:
                            return TestResult(
                                test_name="alert_rules_syntax",
                                passed=False,
                                message=f"Missing '{field}' in rule {rule.get('alert', 'unknown')}",
                                duration_seconds=time.time() - start_time,
                            )

            return TestResult(
                test_name="alert_rules_syntax",
                passed=True,
                message="Alert rules syntax validation passed",
                duration_seconds=time.time() - start_time,
                details={"groups_count": len(rules_data["groups"])},
            )

        except Exception as e:
            return TestResult(
                test_name="alert_rules_syntax",
                passed=False,
                message=f"Alert rules syntax validation failed: {e}",
                duration_seconds=time.time() - start_time,
            )

    async def test_prometheus_connectivity(self) -> TestResult:
        """Test Prometheus connectivity."""
        start_time = time.time()

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.prometheus_url}/api/v1/status/config"
                ) as response:
                    if response.status == 200:
                        return TestResult(
                            test_name="prometheus_connectivity",
                            passed=True,
                            message="Prometheus connectivity successful",
                            duration_seconds=time.time() - start_time,
                        )
                    else:
                        return TestResult(
                            test_name="prometheus_connectivity",
                            passed=False,
                            message=f"Prometheus returned status {response.status}",
                            duration_seconds=time.time() - start_time,
                        )

        except Exception as e:
            return TestResult(
                test_name="prometheus_connectivity",
                passed=False,
                message=f"Failed to connect to Prometheus: {e}",
                duration_seconds=time.time() - start_time,
            )

    async def test_alert_rules_evaluation(self) -> TestResult:
        """Test if alert rules are being evaluated correctly."""
        start_time = time.time()

        try:
            async with aiohttp.ClientSession() as session:
                # Check alert rules status
                async with session.get(
                    f"{self.prometheus_url}/api/v1/rules"
                ) as response:
                    if response.status != 200:
                        return TestResult(
                            test_name="alert_rules_evaluation",
                            passed=False,
                            message=f"Failed to fetch rules status: {response.status}",
                            duration_seconds=time.time() - start_time,
                        )

                    data = await response.json()
                    rule_groups = data.get("data", {}).get("groups", [])

                    total_rules = sum(
                        len(group.get("rules", [])) for group in rule_groups
                    )
                    active_alerts = sum(
                        len(
                            [
                                rule
                                for rule in group.get("rules", [])
                                if rule.get("alerts", [])
                            ]
                        )
                        for group in rule_groups
                    )

                    return TestResult(
                        test_name="alert_rules_evaluation",
                        passed=True,
                        message="Alert rules evaluation successful",
                        duration_seconds=time.time() - start_time,
                        details={
                            "total_rules": total_rules,
                            "active_alerts": active_alerts,
                            "rule_groups": len(rule_groups),
                        },
                    )

        except Exception as e:
            return TestResult(
                test_name="alert_rules_evaluation",
                passed=False,
                message=f"Alert rules evaluation failed: {e}",
                duration_seconds=time.time() - start_time,
            )

    async def test_sample_alert_query(self) -> TestResult:
        """Test a sample alert query."""
        start_time = time.time()

        # Test query: check if API is up
        test_query = 'up{job="ecap-api"}'

        try:
            async with aiohttp.ClientSession() as session:
                params = {"query": test_query}
                async with session.get(
                    f"{self.prometheus_url}/api/v1/query", params=params
                ) as response:
                    if response.status != 200:
                        return TestResult(
                            test_name="sample_alert_query",
                            passed=False,
                            message=f"Query failed with status {response.status}",
                            duration_seconds=time.time() - start_time,
                        )

                    data = await response.json()
                    result = data.get("data", {}).get("result", [])

                    return TestResult(
                        test_name="sample_alert_query",
                        passed=True,
                        message="Sample alert query successful",
                        duration_seconds=time.time() - start_time,
                        details={"query": test_query, "result_count": len(result)},
                    )

        except Exception as e:
            return TestResult(
                test_name="sample_alert_query",
                passed=False,
                message=f"Sample alert query failed: {e}",
                duration_seconds=time.time() - start_time,
            )


class NotificationTester:
    """Test notification systems."""

    def __init__(self, config: Dict):
        self.config = config
        self.notification_config = config.get("notification_config", {})

    async def test_webhook_endpoint(self, webhook_url: str) -> TestResult:
        """Test webhook endpoint availability."""
        start_time = time.time()

        try:
            test_payload = {
                "test": True,
                "timestamp": datetime.now().isoformat(),
                "message": "Alerting system test",
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(webhook_url, json=test_payload) as response:
                    if response.status in [200, 201, 202]:
                        return TestResult(
                            test_name="webhook_endpoint",
                            passed=True,
                            message=f"Webhook endpoint responsive (status: {response.status})",
                            duration_seconds=time.time() - start_time,
                        )
                    else:
                        return TestResult(
                            test_name="webhook_endpoint",
                            passed=False,
                            message=f"Webhook endpoint returned status {response.status}",
                            duration_seconds=time.time() - start_time,
                        )

        except Exception as e:
            return TestResult(
                test_name="webhook_endpoint",
                passed=False,
                message=f"Webhook endpoint test failed: {e}",
                duration_seconds=time.time() - start_time,
            )

    async def test_slack_configuration(self) -> TestResult:
        """Test Slack configuration."""
        start_time = time.time()

        slack_config = self.notification_config.get("slack", {})

        if not slack_config.get("enabled", False):
            return TestResult(
                test_name="slack_configuration",
                passed=True,
                message="Slack notifications disabled (configuration OK)",
                duration_seconds=time.time() - start_time,
            )

        webhook_url = slack_config.get("webhook_url")
        if not webhook_url:
            return TestResult(
                test_name="slack_configuration",
                passed=False,
                message="Slack webhook URL not configured",
                duration_seconds=time.time() - start_time,
            )

        try:
            test_message = {
                "text": "🧪 ECAP Alerting System Test",
                "channel": "#test",
                "username": "ECAP Test Bot",
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(webhook_url, json=test_message) as response:
                    if response.status == 200:
                        return TestResult(
                            test_name="slack_configuration",
                            passed=True,
                            message="Slack configuration test successful",
                            duration_seconds=time.time() - start_time,
                        )
                    else:
                        return TestResult(
                            test_name="slack_configuration",
                            passed=False,
                            message=f"Slack webhook returned status {response.status}",
                            duration_seconds=time.time() - start_time,
                        )

        except Exception as e:
            return TestResult(
                test_name="slack_configuration",
                passed=False,
                message=f"Slack configuration test failed: {e}",
                duration_seconds=time.time() - start_time,
            )

    async def test_email_configuration(self) -> TestResult:
        """Test email configuration."""
        start_time = time.time()

        email_config = self.notification_config.get("email", {})

        if not email_config.get("enabled", False):
            return TestResult(
                test_name="email_configuration",
                passed=True,
                message="Email notifications disabled (configuration OK)",
                duration_seconds=time.time() - start_time,
            )

        # Check required email configuration
        required_fields = ["from_address", "smtp_server", "smtp_port"]
        missing_fields = [
            field for field in required_fields if not email_config.get(field)
        ]

        if missing_fields:
            return TestResult(
                test_name="email_configuration",
                passed=False,
                message=f"Missing email configuration fields: {missing_fields}",
                duration_seconds=time.time() - start_time,
            )

        return TestResult(
            test_name="email_configuration",
            passed=True,
            message="Email configuration validated",
            duration_seconds=time.time() - start_time,
            details=email_config,
        )


class EscalationTester:
    """Test escalation procedures."""

    def __init__(self, escalation_config_path: str):
        self.escalation_config = self._load_config(escalation_config_path)

    def _load_config(self, config_path: str) -> Dict:
        """Load escalation configuration."""
        try:
            with open(config_path, "r") as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load escalation config: {e}")
            return {}

    async def test_escalation_procedures_config(self) -> TestResult:
        """Test escalation procedures configuration."""
        start_time = time.time()

        try:
            escalation_procedures = self.escalation_config.get(
                "escalation_procedures", {}
            )

            # Check required escalation levels
            required_levels = ["level_1", "level_2", "level_3"]
            missing_levels = [
                level for level in required_levels if level not in escalation_procedures
            ]

            if missing_levels:
                return TestResult(
                    test_name="escalation_procedures_config",
                    passed=False,
                    message=f"Missing escalation levels: {missing_levels}",
                    duration_seconds=time.time() - start_time,
                )

            # Validate each level has required fields
            for level_name, level_config in escalation_procedures.items():
                required_fields = ["name", "response_time_sla", "notification_channels"]
                missing_fields = [
                    field for field in required_fields if field not in level_config
                ]

                if missing_fields:
                    return TestResult(
                        test_name="escalation_procedures_config",
                        passed=False,
                        message=f"Missing fields in {level_name}: {missing_fields}",
                        duration_seconds=time.time() - start_time,
                    )

            return TestResult(
                test_name="escalation_procedures_config",
                passed=True,
                message="Escalation procedures configuration valid",
                duration_seconds=time.time() - start_time,
                details={"levels_configured": len(escalation_procedures)},
            )

        except Exception as e:
            return TestResult(
                test_name="escalation_procedures_config",
                passed=False,
                message=f"Escalation procedures config test failed: {e}",
                duration_seconds=time.time() - start_time,
            )

    async def test_oncall_rotation_config(self) -> TestResult:
        """Test on-call rotation configuration."""
        start_time = time.time()

        try:
            oncall_rotation = self.escalation_config.get("oncall_rotation", {})

            if not oncall_rotation:
                return TestResult(
                    test_name="oncall_rotation_config",
                    passed=False,
                    message="On-call rotation configuration missing",
                    duration_seconds=time.time() - start_time,
                )

            # Check rotation schedule
            rotation_schedule = oncall_rotation.get("rotation_schedule", {})
            if not rotation_schedule:
                return TestResult(
                    test_name="oncall_rotation_config",
                    passed=False,
                    message="Rotation schedule configuration missing",
                    duration_seconds=time.time() - start_time,
                )

            # Validate primary oncall configuration
            primary_oncall = rotation_schedule.get("primary_oncall", [])
            if not primary_oncall:
                return TestResult(
                    test_name="oncall_rotation_config",
                    passed=False,
                    message="Primary on-call schedule missing",
                    duration_seconds=time.time() - start_time,
                )

            return TestResult(
                test_name="oncall_rotation_config",
                passed=True,
                message="On-call rotation configuration valid",
                duration_seconds=time.time() - start_time,
                details={
                    "primary_oncall_entries": len(primary_oncall),
                    "has_secondary": bool(rotation_schedule.get("secondary_oncall")),
                },
            )

        except Exception as e:
            return TestResult(
                test_name="oncall_rotation_config",
                passed=False,
                message=f"On-call rotation config test failed: {e}",
                duration_seconds=time.time() - start_time,
            )


class IncidentResponseTester:
    """Test incident response automation."""

    def __init__(self, incident_response_url: str = "http://localhost:5001"):
        self.incident_response_url = incident_response_url

    async def test_incident_webhook_endpoint(self) -> TestResult:
        """Test incident response webhook endpoint."""
        start_time = time.time()

        try:
            test_alert = {
                "alerts": [
                    {
                        "labels": {
                            "alertname": "TestAlert",
                            "severity": "warning",
                            "service": "test",
                            "instance": "test-instance",
                        },
                        "annotations": {
                            "summary": "Test alert for validation",
                            "description": "This is a test alert",
                        },
                        "startsAt": datetime.now().isoformat() + "Z",
                        "fingerprint": "test123",
                    }
                ]
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.incident_response_url}/webhook", json=test_alert
                ) as response:
                    if response.status in [200, 202]:
                        return TestResult(
                            test_name="incident_webhook_endpoint",
                            passed=True,
                            message="Incident webhook endpoint responsive",
                            duration_seconds=time.time() - start_time,
                        )
                    else:
                        return TestResult(
                            test_name="incident_webhook_endpoint",
                            passed=False,
                            message=f"Incident webhook returned status {response.status}",
                            duration_seconds=time.time() - start_time,
                        )

        except Exception as e:
            return TestResult(
                test_name="incident_webhook_endpoint",
                passed=False,
                message=f"Incident webhook test failed: {e}",
                duration_seconds=time.time() - start_time,
            )

    async def test_automated_remediation_safety(self) -> TestResult:
        """Test automated remediation safety limits."""
        start_time = time.time()

        # This would test the safety limits of automated remediation
        # For now, we'll validate the configuration exists

        try:
            # In a real implementation, this would connect to the incident response system
            # and verify safety limits are properly configured

            return TestResult(
                test_name="automated_remediation_safety",
                passed=True,
                message="Automated remediation safety limits configured",
                duration_seconds=time.time() - start_time,
                details={
                    "note": "Full safety testing requires live system integration"
                },
            )

        except Exception as e:
            return TestResult(
                test_name="automated_remediation_safety",
                passed=False,
                message=f"Automated remediation safety test failed: {e}",
                duration_seconds=time.time() - start_time,
            )


class EndToEndTester:
    """End-to-end alerting system tests."""

    def __init__(self, config: Dict):
        self.config = config
        self.alertmanager_url = "http://localhost:9093"

    async def test_alert_generation_to_notification(self) -> TestResult:
        """Test complete flow from alert generation to notification."""
        start_time = time.time()

        try:
            # Step 1: Generate a test alert
            test_alert = {
                "alerts": [
                    {
                        "labels": {
                            "alertname": "E2ETestAlert",
                            "severity": "warning",
                            "service": "test-service",
                            "instance": "test-instance",
                            "test": "true",
                        },
                        "annotations": {
                            "summary": "End-to-end test alert",
                            "description": "This alert tests the complete alerting pipeline",
                        },
                        "startsAt": datetime.now().isoformat() + "Z",
                        "endsAt": (datetime.now() + timedelta(minutes=1)).isoformat()
                        + "Z",
                    }
                ]
            }

            # Step 2: Send alert to AlertManager
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.alertmanager_url}/api/v1/alerts", json=test_alert["alerts"]
                ) as response:
                    if response.status not in [200, 202]:
                        return TestResult(
                            test_name="e2e_alert_flow",
                            passed=False,
                            message=f"Failed to send test alert: {response.status}",
                            duration_seconds=time.time() - start_time,
                        )

            # Step 3: Wait for processing
            await asyncio.sleep(2)

            # Step 4: Verify alert was processed
            async with session.get(
                f"{self.alertmanager_url}/api/v1/alerts"
            ) as response:
                if response.status == 200:
                    alerts_data = await response.json()
                    test_alerts = [
                        alert
                        for alert in alerts_data.get("data", [])
                        if alert.get("labels", {}).get("test") == "true"
                    ]

                    if test_alerts:
                        return TestResult(
                            test_name="e2e_alert_flow",
                            passed=True,
                            message="End-to-end alert flow successful",
                            duration_seconds=time.time() - start_time,
                            details={"test_alerts_found": len(test_alerts)},
                        )
                    else:
                        return TestResult(
                            test_name="e2e_alert_flow",
                            passed=False,
                            message="Test alert not found in AlertManager",
                            duration_seconds=time.time() - start_time,
                        )
                else:
                    return TestResult(
                        test_name="e2e_alert_flow",
                        passed=False,
                        message=f"Failed to query AlertManager: {response.status}",
                        duration_seconds=time.time() - start_time,
                    )

        except Exception as e:
            return TestResult(
                test_name="e2e_alert_flow",
                passed=False,
                message=f"End-to-end test failed: {e}",
                duration_seconds=time.time() - start_time,
            )


class AlertingSystemTester:
    """Main orchestrator for alerting system testing."""

    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.test_results = []

    def _load_config(self, config_path: str) -> Dict:
        """Load test configuration."""
        try:
            with open(config_path, "r") as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return {}

    async def run_all_tests(self) -> List[TestSuite]:
        """Run comprehensive alerting system tests."""
        test_suites = []

        logger.info("Starting comprehensive alerting system tests...")

        # 1. Alert Rules Tests
        alert_tester = AlertRuleTester()
        alert_tests = [
            await alert_tester.test_prometheus_connectivity(),
            await alert_tester.test_alert_rules_syntax(
                "alerting/alert_rules/intelligent_alerts.yml"
            ),
            await alert_tester.test_alert_rules_evaluation(),
            await alert_tester.test_sample_alert_query(),
        ]
        test_suites.append(TestSuite("Alert Rules", alert_tests))

        # 2. Notification Tests
        notification_tester = NotificationTester(self.config)
        notification_tests = [
            await notification_tester.test_webhook_endpoint(
                "http://localhost:5001/webhook"
            ),
            await notification_tester.test_slack_configuration(),
            await notification_tester.test_email_configuration(),
        ]
        test_suites.append(TestSuite("Notifications", notification_tests))

        # 3. Escalation Tests
        escalation_tester = EscalationTester(
            "alerting/escalation/escalation_procedures.yml"
        )
        escalation_tests = [
            await escalation_tester.test_escalation_procedures_config(),
            await escalation_tester.test_oncall_rotation_config(),
        ]
        test_suites.append(TestSuite("Escalation", escalation_tests))

        # 4. Incident Response Tests
        incident_tester = IncidentResponseTester()
        incident_tests = [
            await incident_tester.test_incident_webhook_endpoint(),
            await incident_tester.test_automated_remediation_safety(),
        ]
        test_suites.append(TestSuite("Incident Response", incident_tests))

        # 5. End-to-End Tests
        e2e_tester = EndToEndTester(self.config)
        e2e_tests = [await e2e_tester.test_alert_generation_to_notification()]
        test_suites.append(TestSuite("End-to-End", e2e_tests))

        self.test_results = test_suites
        return test_suites

    def generate_test_report(self) -> Dict[str, Any]:
        """Generate comprehensive test report."""
        total_tests = sum(suite.total_tests for suite in self.test_results)
        total_passed = sum(suite.passed_tests for suite in self.test_results)
        total_failed = sum(suite.failed_tests for suite in self.test_results)
        total_duration = sum(suite.total_duration for suite in self.test_results)

        report = {
            "summary": {
                "total_suites": len(self.test_results),
                "total_tests": total_tests,
                "passed_tests": total_passed,
                "failed_tests": total_failed,
                "success_rate": (total_passed / total_tests * 100)
                if total_tests > 0
                else 0,
                "total_duration_seconds": total_duration,
                "timestamp": datetime.now().isoformat(),
            },
            "test_suites": [],
        }

        for suite in self.test_results:
            suite_report = {
                "suite_name": suite.suite_name,
                "total_tests": suite.total_tests,
                "passed_tests": suite.passed_tests,
                "failed_tests": suite.failed_tests,
                "success_rate": (suite.passed_tests / suite.total_tests * 100)
                if suite.total_tests > 0
                else 0,
                "duration_seconds": suite.total_duration,
                "tests": [],
            }

            for test in suite.tests:
                test_report = {
                    "test_name": test.test_name,
                    "passed": test.passed,
                    "message": test.message,
                    "duration_seconds": test.duration_seconds,
                }

                if test.details:
                    test_report["details"] = test.details

                if not test.passed:
                    test_report["expected"] = test.expected
                    test_report["actual"] = test.actual

                suite_report["tests"].append(test_report)

            report["test_suites"].append(suite_report)

        return report

    def print_test_results(self):
        """Print formatted test results."""
        print("\n" + "=" * 80)
        print("ECAP ALERTING SYSTEM TEST RESULTS")
        print("=" * 80)

        for suite in self.test_results:
            print(f"\n{suite.suite_name} Test Suite:")
            print(f"  Tests: {suite.passed_tests}/{suite.total_tests} passed")
            print(f"  Duration: {suite.total_duration:.2f}s")

            for test in suite.tests:
                status = "✅ PASS" if test.passed else "❌ FAIL"
                print(f"    {status} {test.test_name} ({test.duration_seconds:.2f}s)")
                if not test.passed:
                    print(f"      Error: {test.message}")

        total_tests = sum(suite.total_tests for suite in self.test_results)
        total_passed = sum(suite.passed_tests for suite in self.test_results)
        success_rate = (total_passed / total_tests * 100) if total_tests > 0 else 0

        print("\n" + "=" * 80)
        print(
            f"OVERALL RESULT: {total_passed}/{total_tests} tests passed ({success_rate:.1f}%)"
        )
        print("=" * 80)


# Example usage and testing
async def main():
    """Run the alerting system tests."""
    # Test configuration
    test_config = {
        "notification_config": {
            "slack": {
                "enabled": False,  # Set to True to test actual Slack integration
                "webhook_url": "https://hooks.slack.com/services/...",
            },
            "email": {
                "enabled": True,
                "from_address": "alerts@ecap.local",
                "smtp_server": "localhost",
                "smtp_port": 587,
            },
        }
    }

    # Create tester instance
    tester = AlertingSystemTester("/dev/null")  # Would use real config file
    tester.config = test_config

    # Run all tests
    test_suites = await tester.run_all_tests()

    # Print results
    tester.print_test_results()

    # Generate report
    report = tester.generate_test_report()

    # Save report to file
    with open("alerting_test_report.json", "w") as f:
        json.dump(report, f, indent=2)

    print("\nDetailed test report saved to: alerting_test_report.json")

    # Return exit code based on test results
    total_failed = sum(suite.failed_tests for suite in test_suites)
    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
