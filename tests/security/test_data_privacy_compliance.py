"""
Data privacy compliance testing framework.

This module provides comprehensive testing for GDPR, CCPA, and other
data privacy regulation compliance.
"""

import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pytest


class DataPrivacyComplianceTester:
    """Comprehensive data privacy compliance testing framework."""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.compliance_violations = []
        self.personal_data_patterns = self._load_personal_data_patterns()
        self.gdpr_requirements = self._load_gdpr_requirements()
        self.ccpa_requirements = self._load_ccpa_requirements()

    def _load_personal_data_patterns(self) -> Dict[str, List[str]]:
        """Load patterns that identify personal data."""
        return {
            "email": [
                r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",
                r"email",
                r"e_mail",
                r"email_address",
            ],
            "phone": [
                r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b",
                r"\b\+\d{1,3}[-.]?\d{3,4}[-.]?\d{3,4}[-.]?\d{3,4}\b",
                r"phone",
                r"telephone",
                r"mobile",
            ],
            "ssn": [
                r"\b\d{3}-\d{2}-\d{4}\b",
                r"ssn",
                r"social_security",
                r"social_security_number",
            ],
            "credit_card": [
                r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b",
                r"credit_card",
                r"card_number",
                r"ccn",
            ],
            "address": [
                r"address",
                r"street",
                r"postal_code",
                r"zip_code",
                r"location",
                r"latitude",
                r"longitude",
            ],
            "name": [
                r"first_name",
                r"last_name",
                r"full_name",
                r"surname",
                r"given_name",
            ],
            "date_of_birth": [r"date_of_birth", r"dob", r"birth_date", r"birthday"],
            "ip_address": [
                r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",
                r"ip_address",
                r"remote_addr",
            ],
        }

    def _load_gdpr_requirements(self) -> Dict[str, Dict]:
        """Load GDPR compliance requirements."""
        return {
            "lawful_basis": {
                "description": "Processing must have a lawful basis",
                "requirements": [
                    "Consent mechanism",
                    "Legitimate interest assessment",
                    "Contract necessity documentation",
                ],
            },
            "data_minimization": {
                "description": "Only collect necessary data",
                "requirements": [
                    "Purpose limitation",
                    "Data retention policies",
                    "Regular data audits",
                ],
            },
            "consent": {
                "description": "Clear and informed consent",
                "requirements": [
                    "Opt-in mechanism",
                    "Withdrawal mechanism",
                    "Granular consent options",
                ],
            },
            "data_subject_rights": {
                "description": "Individual rights must be supported",
                "requirements": [
                    "Right to access",
                    "Right to rectification",
                    "Right to erasure",
                    "Right to portability",
                    "Right to object",
                ],
            },
            "data_protection_by_design": {
                "description": "Privacy by design principles",
                "requirements": [
                    "Encryption at rest",
                    "Encryption in transit",
                    "Access controls",
                    "Audit logging",
                ],
            },
            "breach_notification": {
                "description": "Data breach procedures",
                "requirements": [
                    "72-hour notification",
                    "Individual notification",
                    "Breach documentation",
                ],
            },
        }

    def _load_ccpa_requirements(self) -> Dict[str, Dict]:
        """Load CCPA compliance requirements."""
        return {
            "consumer_rights": {
                "description": "Consumer privacy rights",
                "requirements": [
                    "Right to know",
                    "Right to delete",
                    "Right to opt-out",
                    "Right to non-discrimination",
                ],
            },
            "privacy_notice": {
                "description": "Privacy notice requirements",
                "requirements": [
                    "Categories of personal information",
                    "Purposes of collection",
                    "Third-party sharing",
                    "Consumer rights disclosure",
                ],
            },
            "opt_out": {
                "description": "Sale opt-out mechanism",
                "requirements": [
                    "Do not sell link",
                    "Opt-out process",
                    "Verification procedures",
                ],
            },
        }

    def test_personal_data_detection(self) -> List[Dict]:
        """Test detection of personal data across the codebase."""
        violations = []

        # Scan source code files
        source_files = list(self.project_root.glob("**/*.py"))
        source_files.extend(list(self.project_root.glob("**/*.sql")))
        source_files.extend(list(self.project_root.glob("**/*.yaml")))
        source_files.extend(list(self.project_root.glob("**/*.yml")))
        source_files.extend(list(self.project_root.glob("**/*.json")))

        for file_path in source_files:
            try:
                if file_path.name.startswith(".") or "__pycache__" in str(file_path):
                    continue

                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()

                file_violations = self._scan_file_for_personal_data(file_path, content)
                violations.extend(file_violations)

            except (UnicodeDecodeError, PermissionError):
                continue

        return violations

    def _scan_file_for_personal_data(self, file_path: Path, content: str) -> List[Dict]:
        """Scan a single file for personal data patterns."""
        violations = []

        for data_type, patterns in self.personal_data_patterns.items():
            for pattern in patterns:
                matches = re.finditer(pattern, content, re.IGNORECASE)
                for match in matches:
                    # Skip if in comments or test data
                    if self._is_safe_context(content, match.start()):
                        continue

                    violations.append(
                        {
                            "type": "personal_data_exposure",
                            "severity": "high",
                            "data_type": data_type,
                            "file": str(file_path.relative_to(self.project_root)),
                            "line": content[: match.start()].count("\n") + 1,
                            "match": match.group(),
                            "pattern": pattern,
                        }
                    )

        return violations

    def _is_safe_context(self, content: str, position: int) -> bool:
        """Check if the match is in a safe context (comments, test data, etc.)."""
        # Get the line containing the match
        lines = content[:position].split("\n")
        current_line = lines[-1] if lines else ""

        # Check if it's in a comment
        if "#" in current_line:
            comment_pos = current_line.find("#")
            line_start = sum(len(line) + 1 for line in lines[:-1])
            if comment_pos < len(current_line) - position + line_start:
                return True

        # Check if it's in test data or examples
        safe_contexts = [
            "example",
            "test",
            "sample",
            "mock",
            "fake",
            "dummy",
            "placeholder",
        ]

        line_lower = current_line.lower()
        return any(context in line_lower for context in safe_contexts)

    def test_gdpr_compliance(self) -> List[Dict]:
        """Test GDPR compliance across the application."""
        violations = []

        # Test each GDPR requirement
        for requirement, details in self.gdpr_requirements.items():
            requirement_violations = self._test_gdpr_requirement(requirement, details)
            violations.extend(requirement_violations)

        return violations

    def _test_gdpr_requirement(self, requirement: str, details: Dict) -> List[Dict]:
        """Test a specific GDPR requirement."""
        violations = []

        if requirement == "consent":
            consent_violations = self._test_consent_mechanisms()
            violations.extend(consent_violations)

        elif requirement == "data_subject_rights":
            rights_violations = self._test_data_subject_rights()
            violations.extend(rights_violations)

        elif requirement == "data_protection_by_design":
            design_violations = self._test_privacy_by_design()
            violations.extend(design_violations)

        elif requirement == "data_minimization":
            minimization_violations = self._test_data_minimization()
            violations.extend(minimization_violations)

        return violations

    def _test_consent_mechanisms(self) -> List[Dict]:
        """Test consent collection and management mechanisms."""
        violations = []

        # Check for consent-related code
        consent_keywords = [
            "consent",
            "agree",
            "accept",
            "opt_in",
            "opt_out",
            "privacy_policy",
            "terms_of_service",
        ]

        found_consent = False
        source_files = list(self.project_root.glob("**/*.py"))

        for file_path in source_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read().lower()

                if any(keyword in content for keyword in consent_keywords):
                    found_consent = True
                    break

            except (UnicodeDecodeError, PermissionError):
                continue

        if not found_consent:
            violations.append(
                {
                    "type": "gdpr_violation",
                    "severity": "high",
                    "requirement": "consent",
                    "description": "No consent mechanism found in codebase",
                    "recommendation": "Implement consent collection and management",
                }
            )

        return violations

    def _test_data_subject_rights(self) -> List[Dict]:
        """Test implementation of data subject rights."""
        violations = []

        required_endpoints = [
            "data_access",
            "data_deletion",
            "data_portability",
            "data_rectification",
        ]

        found_endpoints = set()
        api_files = list(self.project_root.glob("**/api/**/*.py"))

        for file_path in api_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read().lower()

                for endpoint in required_endpoints:
                    if endpoint in content or endpoint.replace("_", "") in content:
                        found_endpoints.add(endpoint)

            except (UnicodeDecodeError, PermissionError):
                continue

        missing_endpoints = set(required_endpoints) - found_endpoints
        for endpoint in missing_endpoints:
            violations.append(
                {
                    "type": "gdpr_violation",
                    "severity": "high",
                    "requirement": "data_subject_rights",
                    "description": f"Missing {endpoint} endpoint for data subject rights",
                    "recommendation": f"Implement {endpoint} API endpoint",
                }
            )

        return violations

    def _test_privacy_by_design(self) -> List[Dict]:
        """Test privacy by design implementation."""
        violations = []

        # Check for encryption usage
        encryption_keywords = ["encrypt", "decrypt", "hash", "bcrypt", "cryptography"]
        found_encryption = False

        source_files = list(self.project_root.glob("**/*.py"))
        for file_path in source_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read().lower()

                if any(keyword in content for keyword in encryption_keywords):
                    found_encryption = True
                    break

            except (UnicodeDecodeError, PermissionError):
                continue

        if not found_encryption:
            violations.append(
                {
                    "type": "gdpr_violation",
                    "severity": "medium",
                    "requirement": "data_protection_by_design",
                    "description": "No encryption mechanisms found",
                    "recommendation": "Implement data encryption at rest and in transit",
                }
            )

        # Check for access control
        access_control_keywords = ["auth", "permission", "role", "access_control"]
        found_access_control = False

        for file_path in source_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read().lower()

                if any(keyword in content for keyword in access_control_keywords):
                    found_access_control = True
                    break

            except (UnicodeDecodeError, PermissionError):
                continue

        if not found_access_control:
            violations.append(
                {
                    "type": "gdpr_violation",
                    "severity": "high",
                    "requirement": "data_protection_by_design",
                    "description": "No access control mechanisms found",
                    "recommendation": "Implement role-based access control",
                }
            )

        return violations

    def _test_data_minimization(self) -> List[Dict]:
        """Test data minimization principles."""
        violations = []

        # Check for data retention policies
        retention_keywords = ["retention", "delete", "purge", "expire", "cleanup"]
        found_retention = False

        source_files = list(self.project_root.glob("**/*.py"))
        for file_path in source_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read().lower()

                if any(keyword in content for keyword in retention_keywords):
                    found_retention = True
                    break

            except (UnicodeDecodeError, PermissionError):
                continue

        if not found_retention:
            violations.append(
                {
                    "type": "gdpr_violation",
                    "severity": "medium",
                    "requirement": "data_minimization",
                    "description": "No data retention policies found",
                    "recommendation": "Implement automatic data retention and deletion",
                }
            )

        return violations

    def test_ccpa_compliance(self) -> List[Dict]:
        """Test CCPA compliance."""
        violations = []

        # Test each CCPA requirement
        for requirement, details in self.ccpa_requirements.items():
            requirement_violations = self._test_ccpa_requirement(requirement, details)
            violations.extend(requirement_violations)

        return violations

    def _test_ccpa_requirement(self, requirement: str, details: Dict) -> List[Dict]:
        """Test a specific CCPA requirement."""
        violations = []

        if requirement == "consumer_rights":
            rights_violations = self._test_ccpa_consumer_rights()
            violations.extend(rights_violations)

        elif requirement == "opt_out":
            opt_out_violations = self._test_ccpa_opt_out()
            violations.extend(opt_out_violations)

        return violations

    def _test_ccpa_consumer_rights(self) -> List[Dict]:
        """Test CCPA consumer rights implementation."""
        violations = []

        required_rights = ["know", "delete", "opt_out", "non_discrimination"]
        found_rights = set()

        source_files = list(self.project_root.glob("**/*.py"))
        for file_path in source_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read().lower()

                for right in required_rights:
                    if right in content:
                        found_rights.add(right)

            except (UnicodeDecodeError, PermissionError):
                continue

        missing_rights = set(required_rights) - found_rights
        for right in missing_rights:
            violations.append(
                {
                    "type": "ccpa_violation",
                    "severity": "high",
                    "requirement": "consumer_rights",
                    "description": f"Missing implementation for right to {right}",
                    "recommendation": f"Implement consumer right to {right}",
                }
            )

        return violations

    def _test_ccpa_opt_out(self) -> List[Dict]:
        """Test CCPA opt-out mechanism."""
        violations = []

        opt_out_keywords = ["do_not_sell", "opt_out", "sale_opt_out"]
        found_opt_out = False

        source_files = list(self.project_root.glob("**/*.py"))
        for file_path in source_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read().lower()

                if any(keyword in content for keyword in opt_out_keywords):
                    found_opt_out = True
                    break

            except (UnicodeDecodeError, PermissionError):
                continue

        if not found_opt_out:
            violations.append(
                {
                    "type": "ccpa_violation",
                    "severity": "high",
                    "requirement": "opt_out",
                    "description": "No opt-out mechanism found for data sales",
                    "recommendation": "Implement 'Do Not Sell My Personal Information' feature",
                }
            )

        return violations

    def test_cookie_compliance(self) -> List[Dict]:
        """Test cookie usage compliance."""
        violations = []

        # Check for cookie usage
        cookie_files = []
        source_files = list(self.project_root.glob("**/*.py"))
        source_files.extend(list(self.project_root.glob("**/*.js")))
        source_files.extend(list(self.project_root.glob("**/*.html")))

        for file_path in source_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read().lower()

                if "cookie" in content:
                    cookie_files.append(file_path)

            except (UnicodeDecodeError, PermissionError):
                continue

        if cookie_files:
            # Check for cookie consent mechanism
            consent_keywords = ["cookie_consent", "accept_cookies", "cookie_banner"]
            found_consent = False

            for file_path in source_files:
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        content = f.read().lower()

                    if any(keyword in content for keyword in consent_keywords):
                        found_consent = True
                        break

                except (UnicodeDecodeError, PermissionError):
                    continue

            if not found_consent:
                violations.append(
                    {
                        "type": "cookie_compliance_violation",
                        "severity": "medium",
                        "description": "Cookie usage found without consent mechanism",
                        "files": [
                            str(f.relative_to(self.project_root))
                            for f in cookie_files[:5]
                        ],
                        "recommendation": "Implement cookie consent banner and management",
                    }
                )

        return violations

    def run_comprehensive_privacy_audit(self) -> Dict:
        """Run comprehensive data privacy compliance audit."""
        print("🔍 Starting comprehensive data privacy compliance audit...")

        all_violations = []

        # Run all compliance tests
        personal_data_violations = self.test_personal_data_detection()
        all_violations.extend(personal_data_violations)

        gdpr_violations = self.test_gdpr_compliance()
        all_violations.extend(gdpr_violations)

        ccpa_violations = self.test_ccpa_compliance()
        all_violations.extend(ccpa_violations)

        cookie_violations = self.test_cookie_compliance()
        all_violations.extend(cookie_violations)

        # Generate comprehensive report
        return self._generate_privacy_compliance_report(all_violations)

    def _generate_privacy_compliance_report(self, violations: List[Dict]) -> Dict:
        """Generate comprehensive privacy compliance report."""
        severity_counts = {"critical": 0, "high": 0, "medium": 0, "low": 0}
        violation_types = {}
        regulation_violations = {"gdpr": 0, "ccpa": 0, "general": 0}

        for violation in violations:
            severity = violation.get("severity", "low")
            if severity in severity_counts:
                severity_counts[severity] += 1

            violation_type = violation.get("type", "unknown")
            violation_types[violation_type] = violation_types.get(violation_type, 0) + 1

            # Categorize by regulation
            if "gdpr" in violation_type:
                regulation_violations["gdpr"] += 1
            elif "ccpa" in violation_type:
                regulation_violations["ccpa"] += 1
            else:
                regulation_violations["general"] += 1

        compliance_score = max(
            0,
            100
            - (
                severity_counts["critical"] * 20
                + severity_counts["high"] * 10
                + severity_counts["medium"] * 5
                + severity_counts["low"] * 2
            ),
        )

        return {
            "audit_timestamp": datetime.now().isoformat(),
            "total_violations": len(violations),
            "compliance_score": compliance_score,
            "severity_breakdown": severity_counts,
            "violation_types": violation_types,
            "regulation_violations": regulation_violations,
            "violations": violations,
            "recommendations": self._generate_privacy_recommendations(violations),
            "compliance_status": self._get_compliance_status(compliance_score),
        }

    def _get_compliance_status(self, score: float) -> str:
        """Get compliance status based on score."""
        if score >= 90:
            return "excellent"
        elif score >= 75:
            return "good"
        elif score >= 60:
            return "fair"
        elif score >= 40:
            return "poor"
        else:
            return "critical"

    def _generate_privacy_recommendations(self, violations: List[Dict]) -> List[str]:
        """Generate privacy compliance recommendations."""
        recommendations = []

        if not violations:
            recommendations.append("✅ No privacy compliance violations found")
            return recommendations

        # Group recommendations by violation type
        violation_types = {v.get("type", "") for v in violations}

        if "personal_data_exposure" in violation_types:
            recommendations.append("🔒 Review and secure personal data handling in code")

        if "gdpr_violation" in violation_types:
            recommendations.append("🇪🇺 Implement missing GDPR compliance requirements")

        if "ccpa_violation" in violation_types:
            recommendations.append("🇺🇸 Implement missing CCPA compliance requirements")

        if "cookie_compliance_violation" in violation_types:
            recommendations.append("🍪 Implement cookie consent and management system")

        recommendations.extend(
            [
                "📋 Conduct regular privacy impact assessments",
                "🎓 Provide privacy training for development team",
                "📝 Document data processing activities",
                "🔄 Establish regular compliance auditing",
                "⚖️ Consult with legal team for regulation updates",
                "🛡️ Implement privacy by design principles",
                "📊 Monitor data processing activities continuously",
            ]
        )

        return recommendations


class TestDataPrivacyCompliance:
    """Test cases for data privacy compliance testing."""

    @pytest.fixture
    def tester(self):
        """Create privacy compliance tester instance."""
        return DataPrivacyComplianceTester()

    def test_personal_data_detection(self, tester):
        """Test personal data detection functionality."""
        violations = tester.test_personal_data_detection()

        assert isinstance(violations, list)
        # Violations may or may not be found - that's expected

    def test_gdpr_compliance_testing(self, tester):
        """Test GDPR compliance testing."""
        violations = tester.test_gdpr_compliance()

        assert isinstance(violations, list)
        # Test should complete without errors

    def test_ccpa_compliance_testing(self, tester):
        """Test CCPA compliance testing."""
        violations = tester.test_ccpa_compliance()

        assert isinstance(violations, list)
        # Test should complete without errors

    def test_cookie_compliance_testing(self, tester):
        """Test cookie compliance testing."""
        violations = tester.test_cookie_compliance()

        assert isinstance(violations, list)
        # Test should complete without errors

    def test_safe_context_detection(self, tester):
        """Test safe context detection for personal data."""
        # Test comment context
        content_with_comment = (
            "# This is an example email: test@example.com\nreal_code = True"
        )
        assert tester._is_safe_context(content_with_comment, 30)

        # Test test data context
        content_with_test = "test_email = 'example@test.com'"
        assert tester._is_safe_context(content_with_test, 15)

    def test_personal_data_pattern_matching(self, tester):
        """Test personal data pattern matching."""
        test_content = """
        user_email = "john.doe@example.com"
        phone_number = "555-123-4567"
        ssn = "123-45-6789"
        """

        from pathlib import Path

        violations = tester._scan_file_for_personal_data(
            Path("test_file.py"), test_content
        )

        # Should find email, phone, and SSN patterns
        assert len(violations) >= 3
        found_types = {v["data_type"] for v in violations}
        assert "email" in found_types

    def test_compliance_score_calculation(self, tester):
        """Test compliance score calculation."""
        mock_violations = [
            {"severity": "critical", "type": "gdpr_violation"},
            {"severity": "high", "type": "ccpa_violation"},
            {"severity": "medium", "type": "personal_data_exposure"},
        ]

        report = tester._generate_privacy_compliance_report(mock_violations)

        # Score should be 100 - (1*20 + 1*10 + 1*5) = 65
        assert report["compliance_score"] == 65
        assert report["compliance_status"] == "fair"

    def test_comprehensive_privacy_audit(self, tester):
        """Test comprehensive privacy audit."""
        report = tester.run_comprehensive_privacy_audit()

        # Validate report structure
        required_fields = [
            "audit_timestamp",
            "total_violations",
            "compliance_score",
            "severity_breakdown",
            "violation_types",
            "regulation_violations",
            "violations",
            "recommendations",
            "compliance_status",
        ]

        for field in required_fields:
            assert field in report

        assert isinstance(report["total_violations"], int)
        assert isinstance(report["compliance_score"], (int, float))
        assert 0 <= report["compliance_score"] <= 100
        assert isinstance(report["violations"], list)
        assert isinstance(report["recommendations"], list)

    def test_gdpr_requirement_testing(self, tester):
        """Test individual GDPR requirement testing."""
        consent_violations = tester._test_consent_mechanisms()
        assert isinstance(consent_violations, list)

        rights_violations = tester._test_data_subject_rights()
        assert isinstance(rights_violations, list)

        design_violations = tester._test_privacy_by_design()
        assert isinstance(design_violations, list)

    def test_ccpa_requirement_testing(self, tester):
        """Test individual CCPA requirement testing."""
        rights_violations = tester._test_ccpa_consumer_rights()
        assert isinstance(rights_violations, list)

        opt_out_violations = tester._test_ccpa_opt_out()
        assert isinstance(opt_out_violations, list)

    def test_privacy_recommendations_generation(self, tester):
        """Test privacy recommendations generation."""
        mock_violations = [
            {"type": "gdpr_violation", "severity": "high"},
            {"type": "personal_data_exposure", "severity": "medium"},
        ]

        recommendations = tester._generate_privacy_recommendations(mock_violations)

        assert isinstance(recommendations, list)
        assert len(recommendations) > 0
        assert any("GDPR" in rec for rec in recommendations)


if __name__ == "__main__":
    # Command-line execution for standalone privacy compliance testing
    tester = DataPrivacyComplianceTester()
    report = tester.run_comprehensive_privacy_audit()

    print("\n🛡️ Data Privacy Compliance Audit Report")
    print("=" * 70)
    print(f"Audit Time: {report['audit_timestamp']}")
    print(
        f"Compliance Score: {report['compliance_score']}/100 ({report['compliance_status'].upper()})"
    )
    print(f"Total Violations: {report['total_violations']}")

    print("\n🔢 Severity Breakdown:")
    for severity, count in report["severity_breakdown"].items():
        if count > 0:
            print(f"  {severity.capitalize()}: {count}")

    print("\n⚖️ Regulation Violations:")
    for regulation, count in report["regulation_violations"].items():
        if count > 0:
            print(f"  {regulation.upper()}: {count}")

    print("\n🎯 Violation Types:")
    for violation_type, count in report["violation_types"].items():
        print(f"  {violation_type.replace('_', ' ').title()}: {count}")

    print("\n💡 Privacy Recommendations:")
    for rec in report["recommendations"]:
        print(f"  {rec}")

    if report["violations"]:
        print("\n⚠️ Sample Violations:")
        for violation in report["violations"][:3]:  # Show first 3
            print(
                f"  - {violation.get('type', 'Unknown')}: {violation.get('description', 'No description')}"
            )
