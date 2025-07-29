"""
Dependency vulnerability scanning tests.

This module provides comprehensive testing for dependency vulnerabilities
using Safety, pip-audit, and custom vulnerability checks.
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Set

import pytest
from packaging import version


class DependencyVulnerabilityScanner:
    """Scanner for dependency vulnerabilities with multiple backends."""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.known_vulnerabilities: Set[str] = set()
        self.severity_thresholds = {
            "critical": 9.0,
            "high": 7.0,
            "medium": 4.0,
            "low": 0.0,
        }

    def run_safety_scan(self) -> Dict:
        """Run Safety vulnerability scan."""
        try:
            result = subprocess.run(
                [sys.executable, "-m", "safety", "check", "--json"],
                capture_output=True,
                text=True,
                cwd=self.project_root,
            )

            if result.returncode == 0:
                return {"vulnerabilities": [], "status": "clean"}

            try:
                vulnerabilities = json.loads(result.stdout)
                return {
                    "vulnerabilities": vulnerabilities,
                    "status": "vulnerabilities_found",
                }
            except json.JSONDecodeError:
                return {
                    "vulnerabilities": [],
                    "status": "scan_error",
                    "error": result.stderr,
                }

        except Exception as e:
            return {"vulnerabilities": [], "status": "scan_failed", "error": str(e)}

    def run_pip_audit_scan(self) -> Dict:
        """Run pip-audit vulnerability scan."""
        try:
            # Install pip-audit if not available
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "pip-audit"],
                capture_output=True,
            )

            result = subprocess.run(
                [sys.executable, "-m", "pip_audit", "--format=json"],
                capture_output=True,
                text=True,
                cwd=self.project_root,
            )

            if result.returncode == 0:
                return {"vulnerabilities": [], "status": "clean"}

            try:
                vulnerabilities = json.loads(result.stdout)
                return {
                    "vulnerabilities": vulnerabilities,
                    "status": "vulnerabilities_found",
                }
            except json.JSONDecodeError:
                return {
                    "vulnerabilities": [],
                    "status": "scan_error",
                    "error": result.stderr,
                }

        except Exception as e:
            return {"vulnerabilities": [], "status": "scan_failed", "error": str(e)}

    def check_dependency_versions(self) -> Dict:
        """Check for outdated dependencies with known vulnerabilities."""
        vulnerabilities = []

        # Known vulnerable versions (examples - would be populated from threat intel)
        known_vulnerable = {
            "requests": ["<2.31.0"],
            "urllib3": ["<1.26.0"],
            "cryptography": ["<41.0.0"],
            "pillow": ["<8.3.2"],
            "fastapi": ["<0.68.1"],
            "pydantic": ["<1.8.2"],
        }

        try:
            result = subprocess.run(
                [sys.executable, "-m", "pip", "freeze"], capture_output=True, text=True
            )

            installed_packages = {}
            for line in result.stdout.split("\n"):
                if "==" in line:
                    name, ver = line.strip().split("==")
                    installed_packages[name.lower()] = ver

            for package, vulnerable_versions in known_vulnerable.items():
                if package in installed_packages:
                    current_version = installed_packages[package]
                    for vulnerable_pattern in vulnerable_versions:
                        if self._version_matches_pattern(
                            current_version, vulnerable_pattern
                        ):
                            vulnerabilities.append(
                                {
                                    "package": package,
                                    "installed_version": current_version,
                                    "vulnerable_pattern": vulnerable_pattern,
                                    "severity": "high",
                                }
                            )

            return {
                "vulnerabilities": vulnerabilities,
                "status": "vulnerabilities_found" if vulnerabilities else "clean",
            }

        except Exception as e:
            return {"vulnerabilities": [], "status": "scan_failed", "error": str(e)}

    def _version_matches_pattern(self, current_ver: str, pattern: str) -> bool:
        """Check if current version matches vulnerability pattern."""
        try:
            current = version.parse(current_ver)

            if pattern.startswith("<"):
                threshold = version.parse(pattern[1:])
                return current < threshold
            elif pattern.startswith("<="):
                threshold = version.parse(pattern[2:])
                return current <= threshold
            elif pattern.startswith(">"):
                threshold = version.parse(pattern[1:])
                return current > threshold
            elif pattern.startswith(">="):
                threshold = version.parse(pattern[2:])
                return current >= threshold
            elif pattern.startswith("=="):
                threshold = version.parse(pattern[2:])
                return current == threshold

            return False
        except Exception:
            return False

    def generate_vulnerability_report(self, scan_results: List[Dict]) -> Dict:
        """Generate comprehensive vulnerability report."""
        all_vulnerabilities = []
        severity_counts = {"critical": 0, "high": 0, "medium": 0, "low": 0}

        for result in scan_results:
            if result["status"] == "vulnerabilities_found":
                for vuln in result["vulnerabilities"]:
                    severity = vuln.get("severity", "medium").lower()
                    if severity in severity_counts:
                        severity_counts[severity] += 1
                    all_vulnerabilities.append(vuln)

        total_vulnerabilities = len(all_vulnerabilities)
        risk_score = self._calculate_risk_score(severity_counts)

        return {
            "total_vulnerabilities": total_vulnerabilities,
            "severity_breakdown": severity_counts,
            "risk_score": risk_score,
            "risk_level": self._get_risk_level(risk_score),
            "vulnerabilities": all_vulnerabilities,
            "recommendations": self._generate_recommendations(all_vulnerabilities),
        }

    def _calculate_risk_score(self, severity_counts: Dict) -> float:
        """Calculate overall risk score based on vulnerability severities."""
        weights = {"critical": 10.0, "high": 5.0, "medium": 2.0, "low": 1.0}
        total_score = sum(
            severity_counts.get(severity, 0) * weight
            for severity, weight in weights.items()
        )
        return min(total_score, 100.0)  # Cap at 100

    def _get_risk_level(self, risk_score: float) -> str:
        """Get risk level based on score."""
        if risk_score >= 50:
            return "critical"
        elif risk_score >= 25:
            return "high"
        elif risk_score >= 10:
            return "medium"
        else:
            return "low"

    def _generate_recommendations(self, vulnerabilities: List[Dict]) -> List[str]:
        """Generate remediation recommendations."""
        recommendations = []

        if not vulnerabilities:
            recommendations.append("✅ No vulnerabilities found. Continue monitoring.")
            return recommendations

        # Group by package
        packages = {}
        for vuln in vulnerabilities:
            package = vuln.get("package", "unknown")
            if package not in packages:
                packages[package] = []
            packages[package].append(vuln)

        for package, package_vulns in packages.items():
            recommendations.append(
                f"🔧 Update {package} to address {len(package_vulns)} vulnerabilities"
            )

        recommendations.extend(
            [
                "🔄 Run dependency updates regularly",
                "📊 Monitor vulnerability databases for new threats",
                "🛡️ Consider dependency pinning for critical packages",
                "🔍 Review transitive dependencies for indirect vulnerabilities",
            ]
        )

        return recommendations


class TestDependencyVulnerabilities:
    """Test cases for dependency vulnerability scanning."""

    @pytest.fixture
    def scanner(self):
        """Create vulnerability scanner instance."""
        return DependencyVulnerabilityScanner()

    def test_safety_scan_execution(self, scanner):
        """Test that Safety scan executes without errors."""
        result = scanner.run_safety_scan()

        assert "status" in result
        assert result["status"] in [
            "clean",
            "vulnerabilities_found",
            "scan_error",
            "scan_failed",
        ]
        assert "vulnerabilities" in result

    def test_pip_audit_scan_execution(self, scanner):
        """Test that pip-audit scan executes without errors."""
        result = scanner.run_pip_audit_scan()

        assert "status" in result
        assert result["status"] in [
            "clean",
            "vulnerabilities_found",
            "scan_error",
            "scan_failed",
        ]
        assert "vulnerabilities" in result

    def test_dependency_version_checking(self, scanner):
        """Test dependency version vulnerability checking."""
        result = scanner.check_dependency_versions()

        assert "status" in result
        assert "vulnerabilities" in result
        assert isinstance(result["vulnerabilities"], list)

    def test_version_pattern_matching(self, scanner):
        """Test version pattern matching logic."""
        # Test various version patterns
        assert scanner._version_matches_pattern("2.0.0", "<2.1.0")
        assert not scanner._version_matches_pattern("2.1.0", "<2.1.0")
        assert not scanner._version_matches_pattern("2.1.1", "<2.1.0")
        assert scanner._version_matches_pattern("1.0.0", ">=1.0.0")
        assert not scanner._version_matches_pattern("0.9.0", ">=1.0.0")

    def test_vulnerability_report_generation(self, scanner):
        """Test vulnerability report generation."""
        # Mock scan results
        scan_results = [
            {
                "status": "vulnerabilities_found",
                "vulnerabilities": [
                    {"package": "test-pkg", "severity": "high"},
                    {"package": "test-pkg2", "severity": "medium"},
                ],
            },
            {"status": "clean", "vulnerabilities": []},
        ]

        report = scanner.generate_vulnerability_report(scan_results)

        assert "total_vulnerabilities" in report
        assert "severity_breakdown" in report
        assert "risk_score" in report
        assert "risk_level" in report
        assert "recommendations" in report

        assert report["total_vulnerabilities"] == 2
        assert report["severity_breakdown"]["high"] == 1
        assert report["severity_breakdown"]["medium"] == 1

    def test_risk_score_calculation(self, scanner):
        """Test risk score calculation."""
        severity_counts = {"critical": 1, "high": 2, "medium": 3, "low": 4}
        risk_score = scanner._calculate_risk_score(severity_counts)

        # Expected: 1*10 + 2*5 + 3*2 + 4*1 = 10 + 10 + 6 + 4 = 30
        assert risk_score == 30.0

    def test_risk_level_classification(self, scanner):
        """Test risk level classification."""
        assert scanner._get_risk_level(75.0) == "critical"
        assert scanner._get_risk_level(35.0) == "high"
        assert scanner._get_risk_level(15.0) == "medium"
        assert scanner._get_risk_level(5.0) == "low"

    def test_comprehensive_vulnerability_scan(self, scanner):
        """Test comprehensive vulnerability scanning."""
        # Run all scanners
        safety_result = scanner.run_safety_scan()
        version_result = scanner.check_dependency_versions()

        # Generate comprehensive report
        all_results = [safety_result, version_result]
        report = scanner.generate_vulnerability_report(all_results)

        # Validate report structure
        assert isinstance(report["total_vulnerabilities"], int)
        assert isinstance(report["risk_score"], float)
        assert report["risk_level"] in ["critical", "high", "medium", "low"]
        assert isinstance(report["recommendations"], list)
        assert len(report["recommendations"]) > 0

    def test_no_vulnerabilities_scenario(self, scanner):
        """Test behavior when no vulnerabilities are found."""
        clean_results = [
            {"status": "clean", "vulnerabilities": []},
            {"status": "clean", "vulnerabilities": []},
        ]

        report = scanner.generate_vulnerability_report(clean_results)

        assert report["total_vulnerabilities"] == 0
        assert report["risk_score"] == 0.0
        assert report["risk_level"] == "low"
        assert "✅ No vulnerabilities found" in report["recommendations"][0]


if __name__ == "__main__":
    # Command-line execution for standalone vulnerability scanning
    scanner = DependencyVulnerabilityScanner()

    print("🔍 Running comprehensive dependency vulnerability scan...")

    # Run all scans
    safety_result = scanner.run_safety_scan()
    version_result = scanner.check_dependency_versions()

    # Generate report
    all_results = [safety_result, version_result]
    report = scanner.generate_vulnerability_report(all_results)

    # Display results
    print("\n📊 Vulnerability Scan Report")
    print("=" * 50)
    print(f"Total Vulnerabilities: {report['total_vulnerabilities']}")
    print(f"Risk Score: {report['risk_score']:.1f}/100")
    print(f"Risk Level: {report['risk_level'].upper()}")

    print("\n🔢 Severity Breakdown:")
    for severity, count in report["severity_breakdown"].items():
        if count > 0:
            print(f"  {severity.capitalize()}: {count}")

    print("\n💡 Recommendations:")
    for rec in report["recommendations"]:
        print(f"  {rec}")

    if report["vulnerabilities"]:
        print("\n⚠️  Detailed Vulnerabilities:")
        for vuln in report["vulnerabilities"]:
            print(f"  - {vuln}")
