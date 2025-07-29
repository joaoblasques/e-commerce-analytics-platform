"""
Security framework validation tests.

This module validates that the security testing framework correctly
identifies and reports vulnerabilities.
"""

import asyncio
from typing import Dict, List

import pytest
from test_data_privacy_compliance import DataPrivacyComplianceTester
from test_dependency_vulnerabilities import DependencyVulnerabilityScanner
from test_penetration_testing import SecurityPenetrationTester
from test_security_regression import SecurityRegressionTester


class SecurityFrameworkValidator:
    """Validates the comprehensive security testing framework."""

    def __init__(self):
        self.dependency_scanner = DependencyVulnerabilityScanner()
        self.penetration_tester = SecurityPenetrationTester()
        self.privacy_tester = DataPrivacyComplianceTester()
        self.regression_tester = SecurityRegressionTester()

    def validate_dependency_scanning(self) -> Dict:
        """Validate dependency vulnerability scanning capabilities."""
        print("🔍 Validating dependency vulnerability scanning...")

        validation_results = {
            "component": "dependency_scanning",
            "tests": [],
            "overall_status": "pass",
        }

        # Test 1: Scanner initialization
        try:
            assert self.dependency_scanner is not None
            validation_results["tests"].append(
                {
                    "name": "scanner_initialization",
                    "status": "pass",
                    "description": "Dependency scanner initializes correctly",
                }
            )
        except Exception as e:
            validation_results["tests"].append(
                {"name": "scanner_initialization", "status": "fail", "error": str(e)}
            )
            validation_results["overall_status"] = "fail"

        # Test 2: Safety scan execution
        try:
            safety_result = self.dependency_scanner.run_safety_scan()
            assert "status" in safety_result
            assert "vulnerabilities" in safety_result
            validation_results["tests"].append(
                {
                    "name": "safety_scan_execution",
                    "status": "pass",
                    "description": f"Safety scan completed with status: {safety_result['status']}",
                }
            )
        except Exception as e:
            validation_results["tests"].append(
                {"name": "safety_scan_execution", "status": "fail", "error": str(e)}
            )
            validation_results["overall_status"] = "fail"

        # Test 3: Version checking
        try:
            version_result = self.dependency_scanner.check_dependency_versions()
            assert "status" in version_result
            assert "vulnerabilities" in version_result
            validation_results["tests"].append(
                {
                    "name": "version_checking",
                    "status": "pass",
                    "description": f"Version checking completed with {len(version_result['vulnerabilities'])} findings",
                }
            )
        except Exception as e:
            validation_results["tests"].append(
                {"name": "version_checking", "status": "fail", "error": str(e)}
            )
            validation_results["overall_status"] = "fail"

        # Test 4: Report generation
        try:
            mock_results = [
                {
                    "status": "vulnerabilities_found",
                    "vulnerabilities": [{"severity": "high"}],
                },
                {"status": "clean", "vulnerabilities": []},
            ]
            report = self.dependency_scanner.generate_vulnerability_report(mock_results)

            required_fields = ["total_vulnerabilities", "risk_score", "recommendations"]
            for field in required_fields:
                assert field in report

            validation_results["tests"].append(
                {
                    "name": "report_generation",
                    "status": "pass",
                    "description": "Vulnerability report generation works correctly",
                }
            )
        except Exception as e:
            validation_results["tests"].append(
                {"name": "report_generation", "status": "fail", "error": str(e)}
            )
            validation_results["overall_status"] = "fail"

        return validation_results

    async def validate_penetration_testing(self) -> Dict:
        """Validate penetration testing capabilities."""
        print("🛡️ Validating penetration testing...")

        validation_results = {
            "component": "penetration_testing",
            "tests": [],
            "overall_status": "pass",
        }

        # Test 1: Tester initialization
        try:
            assert self.penetration_tester is not None
            assert hasattr(self.penetration_tester, "test_payloads")
            validation_results["tests"].append(
                {
                    "name": "tester_initialization",
                    "status": "pass",
                    "description": "Penetration tester initializes with attack payloads",
                }
            )
        except Exception as e:
            validation_results["tests"].append(
                {"name": "tester_initialization", "status": "fail", "error": str(e)}
            )
            validation_results["overall_status"] = "fail"

        # Test 2: SQL injection detection
        try:
            from unittest.mock import Mock

            sql_error_response = Mock()
            sql_error_response.text = (
                "MySQL error: You have an error in your SQL syntax"
            )

            detection_result = self.penetration_tester._detect_sql_injection_response(
                sql_error_response
            )
            assert detection_result

            validation_results["tests"].append(
                {
                    "name": "sql_injection_detection",
                    "status": "pass",
                    "description": "SQL injection detection works correctly",
                }
            )
        except Exception as e:
            validation_results["tests"].append(
                {"name": "sql_injection_detection", "status": "fail", "error": str(e)}
            )
            validation_results["overall_status"] = "fail"

        # Test 3: Command injection detection
        try:
            from unittest.mock import Mock

            cmd_response = Mock()
            cmd_response.text = "uid=0(root) gid=0(root) groups=0(root)"

            detection_result = (
                self.penetration_tester._detect_command_injection_response(
                    cmd_response, "; id"
                )
            )
            assert detection_result

            validation_results["tests"].append(
                {
                    "name": "command_injection_detection",
                    "status": "pass",
                    "description": "Command injection detection works correctly",
                }
            )
        except Exception as e:
            validation_results["tests"].append(
                {
                    "name": "command_injection_detection",
                    "status": "fail",
                    "error": str(e),
                }
            )
            validation_results["overall_status"] = "fail"

        # Test 4: Authentication testing
        try:
            auth_vulns = await self.penetration_tester.test_authentication_security()
            assert isinstance(auth_vulns, list)

            validation_results["tests"].append(
                {
                    "name": "authentication_testing",
                    "status": "pass",
                    "description": f"Authentication testing completed with {len(auth_vulns)} findings",
                }
            )
        except Exception as e:
            validation_results["tests"].append(
                {"name": "authentication_testing", "status": "fail", "error": str(e)}
            )
            validation_results["overall_status"] = "fail"

        # Test 5: Report generation
        try:
            mock_vulnerabilities = [
                {"type": "sql_injection", "severity": "critical"},
                {"type": "xss", "severity": "high"},
            ]
            report = self.penetration_tester._generate_penetration_report(
                mock_vulnerabilities
            )

            required_fields = ["total_vulnerabilities", "risk_score", "recommendations"]
            for field in required_fields:
                assert field in report

            validation_results["tests"].append(
                {
                    "name": "penetration_report_generation",
                    "status": "pass",
                    "description": "Penetration test report generation works correctly",
                }
            )
        except Exception as e:
            validation_results["tests"].append(
                {
                    "name": "penetration_report_generation",
                    "status": "fail",
                    "error": str(e),
                }
            )
            validation_results["overall_status"] = "fail"

        return validation_results

    def validate_privacy_compliance(self) -> Dict:
        """Validate data privacy compliance testing."""
        print("🔒 Validating privacy compliance testing...")

        validation_results = {
            "component": "privacy_compliance",
            "tests": [],
            "overall_status": "pass",
        }

        # Test 1: Tester initialization
        try:
            assert self.privacy_tester is not None
            assert hasattr(self.privacy_tester, "personal_data_patterns")
            assert hasattr(self.privacy_tester, "gdpr_requirements")
            validation_results["tests"].append(
                {
                    "name": "privacy_tester_initialization",
                    "status": "pass",
                    "description": "Privacy tester initializes with compliance requirements",
                }
            )
        except Exception as e:
            validation_results["tests"].append(
                {
                    "name": "privacy_tester_initialization",
                    "status": "fail",
                    "error": str(e),
                }
            )
            validation_results["overall_status"] = "fail"

        # Test 2: Personal data detection
        try:
            # Create a test file within the project directory
            test_content = """
            user_email = "john.doe@example.com"
            phone_number = "555-123-4567"
            ssn = "123-45-6789"
            """

            # Use a temporary file within the project directory
            temp_file = (
                self.privacy_tester.project_root
                / "tests"
                / "security"
                / "temp_test_file.py"
            )
            temp_file.write_text(test_content)

            try:
                violations = self.privacy_tester._scan_file_for_personal_data(
                    temp_file, test_content
                )
                assert len(violations) > 0  # Should detect email, phone, SSN

                validation_results["tests"].append(
                    {
                        "name": "personal_data_detection",
                        "status": "pass",
                        "description": f"Personal data detection found {len(violations)} issues",
                    }
                )
            finally:
                if temp_file.exists():
                    temp_file.unlink()  # Clean up

        except Exception as e:
            validation_results["tests"].append(
                {"name": "personal_data_detection", "status": "fail", "error": str(e)}
            )
            validation_results["overall_status"] = "fail"

        # Test 3: GDPR compliance testing
        try:
            gdpr_violations = self.privacy_tester.test_gdpr_compliance()
            assert isinstance(gdpr_violations, list)

            validation_results["tests"].append(
                {
                    "name": "gdpr_compliance_testing",
                    "status": "pass",
                    "description": f"GDPR compliance testing completed with {len(gdpr_violations)} findings",
                }
            )
        except Exception as e:
            validation_results["tests"].append(
                {"name": "gdpr_compliance_testing", "status": "fail", "error": str(e)}
            )
            validation_results["overall_status"] = "fail"

        # Test 4: CCPA compliance testing
        try:
            ccpa_violations = self.privacy_tester.test_ccpa_compliance()
            assert isinstance(ccpa_violations, list)

            validation_results["tests"].append(
                {
                    "name": "ccpa_compliance_testing",
                    "status": "pass",
                    "description": f"CCPA compliance testing completed with {len(ccpa_violations)} findings",
                }
            )
        except Exception as e:
            validation_results["tests"].append(
                {"name": "ccpa_compliance_testing", "status": "fail", "error": str(e)}
            )
            validation_results["overall_status"] = "fail"

        # Test 5: Privacy report generation
        try:
            mock_violations = [
                {"type": "gdpr_violation", "severity": "high"},
                {"type": "personal_data_exposure", "severity": "medium"},
            ]
            report = self.privacy_tester._generate_privacy_compliance_report(
                mock_violations
            )

            required_fields = [
                "compliance_score",
                "total_violations",
                "recommendations",
            ]
            for field in required_fields:
                assert field in report

            validation_results["tests"].append(
                {
                    "name": "privacy_report_generation",
                    "status": "pass",
                    "description": "Privacy compliance report generation works correctly",
                }
            )
        except Exception as e:
            validation_results["tests"].append(
                {"name": "privacy_report_generation", "status": "fail", "error": str(e)}
            )
            validation_results["overall_status"] = "fail"

        return validation_results

    def validate_regression_testing(self) -> Dict:
        """Validate security regression testing."""
        print("🔄 Validating regression testing...")

        validation_results = {
            "component": "regression_testing",
            "tests": [],
            "overall_status": "pass",
        }

        # Test 1: Tester initialization and database setup
        try:
            assert self.regression_tester is not None
            assert self.regression_tester.regression_db.exists()

            validation_results["tests"].append(
                {
                    "name": "regression_tester_initialization",
                    "status": "pass",
                    "description": "Regression tester and database initialized correctly",
                }
            )
        except Exception as e:
            validation_results["tests"].append(
                {
                    "name": "regression_tester_initialization",
                    "status": "fail",
                    "error": str(e),
                }
            )
            validation_results["overall_status"] = "fail"

        # Test 2: Codebase hash calculation
        try:
            hash1 = self.regression_tester._calculate_codebase_hash()
            hash2 = self.regression_tester._calculate_codebase_hash()

            assert hash1 == hash2  # Should be consistent
            assert len(hash1) == 64  # SHA256 length

            validation_results["tests"].append(
                {
                    "name": "codebase_hash_calculation",
                    "status": "pass",
                    "description": "Codebase hash calculation works consistently",
                }
            )
        except Exception as e:
            validation_results["tests"].append(
                {"name": "codebase_hash_calculation", "status": "fail", "error": str(e)}
            )
            validation_results["overall_status"] = "fail"

        # Test 3: Baseline creation
        try:
            baseline = self.regression_tester.create_security_baseline()

            expected_keys = [
                "created_at",
                "code_hash",
                "dependency_scan",
                "static_analysis",
                "configuration_scan",
                "secret_scan",
                "permission_scan",
            ]

            for key in expected_keys:
                assert key in baseline

            validation_results["tests"].append(
                {
                    "name": "baseline_creation",
                    "status": "pass",
                    "description": "Security baseline creation works correctly",
                }
            )
        except Exception as e:
            validation_results["tests"].append(
                {"name": "baseline_creation", "status": "fail", "error": str(e)}
            )
            validation_results["overall_status"] = "fail"

        # Test 4: Regression comparison
        try:
            # Mock baseline and current state for testing
            baseline = {
                "dependency_scan": {"vulnerabilities_count": 2},
                "static_analysis": {"issues_count": 5},
                "secret_scan": {"secrets_count": 0},
            }

            current = {
                "dependency_scan": {"vulnerabilities_count": 3},  # Regression
                "static_analysis": {"issues_count": 3},  # Improvement
                "secret_scan": {"secrets_count": 1},  # Critical regression
                "tested_at": "2025-07-29T12:00:00",
            }

            result = self.regression_tester._compare_against_baseline(baseline, current)

            assert "status" in result
            assert "regressions" in result
            assert len(result["regressions"]) > 0

            validation_results["tests"].append(
                {
                    "name": "regression_comparison",
                    "status": "pass",
                    "description": "Regression comparison logic works correctly",
                }
            )
        except Exception as e:
            validation_results["tests"].append(
                {"name": "regression_comparison", "status": "fail", "error": str(e)}
            )
            validation_results["overall_status"] = "fail"

        # Test 5: Continuous monitoring
        try:
            result = self.regression_tester.run_continuous_regression_monitoring()

            assert "status" in result
            assert "action" in result
            assert result["action"] in ["proceed", "block_deployment"]

            validation_results["tests"].append(
                {
                    "name": "continuous_monitoring",
                    "status": "pass",
                    "description": f"Continuous monitoring works with status: {result['status']}",
                }
            )
        except Exception as e:
            validation_results["tests"].append(
                {"name": "continuous_monitoring", "status": "fail", "error": str(e)}
            )
            validation_results["overall_status"] = "fail"

        return validation_results

    async def run_comprehensive_framework_validation(self) -> Dict:
        """Run comprehensive validation of the entire security framework."""
        print("🚀 Running comprehensive security framework validation...")

        validation_report = {
            "validation_timestamp": "2025-07-29T12:00:00",
            "framework_version": "1.0.0",
            "components": [],
            "overall_status": "pass",
            "summary": {},
        }

        # Validate each component
        dependency_results = self.validate_dependency_scanning()
        validation_report["components"].append(dependency_results)

        penetration_results = await self.validate_penetration_testing()
        validation_report["components"].append(penetration_results)

        privacy_results = self.validate_privacy_compliance()
        validation_report["components"].append(privacy_results)

        regression_results = self.validate_regression_testing()
        validation_report["components"].append(regression_results)

        # Calculate summary
        total_tests = sum(
            len(comp["tests"]) for comp in validation_report["components"]
        )
        failed_components = [
            comp
            for comp in validation_report["components"]
            if comp["overall_status"] == "fail"
        ]
        failed_tests = []

        for comp in validation_report["components"]:
            failed_tests.extend(
                [test for test in comp["tests"] if test["status"] == "fail"]
            )

        validation_report["summary"] = {
            "total_components": len(validation_report["components"]),
            "passed_components": len(validation_report["components"])
            - len(failed_components),
            "failed_components": len(failed_components),
            "total_tests": total_tests,
            "passed_tests": total_tests - len(failed_tests),
            "failed_tests": len(failed_tests),
        }

        # Set overall status
        if failed_components:
            validation_report["overall_status"] = "fail"

        return validation_report

    def generate_validation_recommendations(self, validation_report: Dict) -> List[str]:
        """Generate recommendations based on validation results."""
        recommendations = []

        if validation_report["overall_status"] == "pass":
            recommendations.append("✅ Security framework validation passed completely")
        else:
            recommendations.append(
                "⚠️ Security framework has validation failures that need attention"
            )

        # Component-specific recommendations
        for component in validation_report["components"]:
            if component["overall_status"] == "fail":
                failed_tests = [
                    test for test in component["tests"] if test["status"] == "fail"
                ]
                for test in failed_tests:
                    recommendations.append(
                        f"🔧 Fix {component['component']}: {test['name']} - {test.get('error', 'Unknown error')}"
                    )

        recommendations.extend(
            [
                "🔄 Run validation tests regularly during development",
                "📊 Monitor security framework performance and accuracy",
                "🎯 Add more test cases based on actual vulnerabilities found",
                "📋 Document any framework limitations or false positives",
                "🛠️ Keep security tools and dependencies updated",
            ]
        )

        return recommendations


class TestSecurityFrameworkValidation:
    """Test cases for security framework validation."""

    @pytest.fixture
    def validator(self):
        """Create security framework validator."""
        return SecurityFrameworkValidator()

    def test_dependency_scanning_validation(self, validator):
        """Test dependency scanning validation."""
        results = validator.validate_dependency_scanning()

        assert "component" in results
        assert "tests" in results
        assert "overall_status" in results
        assert results["component"] == "dependency_scanning"

    @pytest.mark.asyncio
    async def test_penetration_testing_validation(self, validator):
        """Test penetration testing validation."""
        results = await validator.validate_penetration_testing()

        assert "component" in results
        assert "tests" in results
        assert "overall_status" in results
        assert results["component"] == "penetration_testing"

    def test_privacy_compliance_validation(self, validator):
        """Test privacy compliance validation."""
        results = validator.validate_privacy_compliance()

        assert "component" in results
        assert "tests" in results
        assert "overall_status" in results
        assert results["component"] == "privacy_compliance"

    def test_regression_testing_validation(self, validator):
        """Test regression testing validation."""
        results = validator.validate_regression_testing()

        assert "component" in results
        assert "tests" in results
        assert "overall_status" in results
        assert results["component"] == "regression_testing"

    @pytest.mark.asyncio
    async def test_comprehensive_framework_validation(self, validator):
        """Test comprehensive framework validation."""
        results = await validator.run_comprehensive_framework_validation()

        required_fields = [
            "validation_timestamp",
            "framework_version",
            "components",
            "overall_status",
            "summary",
        ]

        for field in required_fields:
            assert field in results

        assert len(results["components"]) == 4  # All 4 components tested
        assert "total_components" in results["summary"]
        assert "total_tests" in results["summary"]

    def test_validation_recommendations(self, validator):
        """Test validation recommendations generation."""
        mock_report = {
            "overall_status": "fail",
            "components": [
                {
                    "component": "test_component",
                    "overall_status": "fail",
                    "tests": [
                        {"name": "test1", "status": "fail", "error": "Test error"}
                    ],
                }
            ],
        }

        recommendations = validator.generate_validation_recommendations(mock_report)

        assert isinstance(recommendations, list)
        assert len(recommendations) > 0
        assert any("Fix test_component" in rec for rec in recommendations)


if __name__ == "__main__":
    # Command-line execution for framework validation
    async def main():
        """Main function for framework validation."""
        validator = SecurityFrameworkValidator()

        print("🔍 Validating Security Testing Framework")
        print("=" * 60)

        report = await validator.run_comprehensive_framework_validation()

        print("\n📊 Validation Report")
        print(f"Status: {report['overall_status'].upper()}")
        print(
            f"Components: {report['summary']['passed_components']}/{report['summary']['total_components']} passed"
        )
        print(
            f"Tests: {report['summary']['passed_tests']}/{report['summary']['total_tests']} passed"
        )

        print("\n🔧 Component Results:")
        for component in report["components"]:
            status_emoji = "✅" if component["overall_status"] == "pass" else "❌"
            print(
                f"  {status_emoji} {component['component']}: {component['overall_status']}"
            )

            if component["overall_status"] == "fail":
                failed_tests = [
                    test for test in component["tests"] if test["status"] == "fail"
                ]
                for test in failed_tests:
                    print(f"    - {test['name']}: {test.get('error', 'Unknown error')}")

        print("\n💡 Recommendations:")
        recommendations = validator.generate_validation_recommendations(report)
        for rec in recommendations:
            print(f"  {rec}")

        return report["overall_status"] == "pass"

    import sys

    success = asyncio.run(main())
    sys.exit(0 if success else 1)
