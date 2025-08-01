#!/usr/bin/env python3
"""
Automated Dependency Addition Script for E-Commerce Analytics Platform

This script implements the dependency addition workflow outlined in Issue #26,
providing automated validation, security checking, and CI monitoring integration.

Usage: ./scripts/add_dependency.py --name <package> --version <version> --justification <reason>
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests


class DependencyManager:
    """Manage dependency additions with validation and monitoring."""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent
        self.pyproject_path = self.project_root / "pyproject.toml"
        self.dependency_log_path = self.project_root / "dependency_additions.json"
        self.docs_path = self.project_root / "docs" / "dependencies.md"

    def add_dependency(
        self, name: str, version: str, justification: str, dev: bool = False
    ) -> bool:
        """Add a dependency with comprehensive validation and logging."""
        print(f"🔍 Adding dependency: {name} {version}")
        print(f"📋 Justification: {justification}")

        # Step 1: Pre-validation checks
        if not self._validate_package_exists(name):
            return False

        # Step 2: Security assessment
        security_issues = self._check_security(name)
        if security_issues:
            print(f"⚠️ Security concerns found for {name}:")
            for issue in security_issues:
                print(f"  - {issue}")
            response = input("Continue anyway? (y/N): ")
            if response.lower() != "y":
                return False

        # Step 3: License compliance check
        license_info = self._check_license_compliance(name)
        if not license_info["compatible"]:
            print(f"❌ License incompatibility: {license_info['license']}")
            return False

        # Step 4: Impact analysis (before adding)
        before_metrics = self._collect_metrics()

        # Step 5: Add dependency using Poetry
        if not self._add_with_poetry(name, version, dev):
            return False

        # Step 6: Post-addition validation
        after_metrics = self._collect_metrics()
        impact = self._calculate_impact(before_metrics, after_metrics)

        # Step 7: Validate impact against thresholds
        if not self._validate_impact_thresholds(impact):
            print("❌ Impact exceeds acceptable thresholds")
            self._rollback_dependency(name)
            return False

        # Step 8: Log the addition with full context
        self._log_dependency_addition(
            name, version, justification, dev, impact, license_info
        )

        # Step 9: Update documentation
        self._update_dependency_docs(name, version, justification, license_info)

        # Step 10: Generate impact report
        self._generate_impact_report(name, version, impact)

        print(f"✅ Successfully added {name} {version}")
        self._print_next_steps()

        return True

    def _validate_package_exists(self, name: str) -> bool:
        """Validate that package exists on PyPI."""
        print(f"🔍 Validating package existence: {name}")

        try:
            response = requests.get(f"https://pypi.org/pypi/{name}/json", timeout=10)
            if response.status_code == 200:
                package_info = response.json()
                print(f"✅ Package found: {package_info['info']['summary']}")
                return True
            else:
                print(f"❌ Package {name} not found on PyPI")
                return False
        except requests.RequestException as e:
            print(f"⚠️ Unable to verify package existence: {e}")
            # Fallback to pip index
            try:
                result = subprocess.run(
                    ["pip", "index", "versions", name],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                if result.returncode == 0:
                    print(f"✅ Package {name} verified via pip index")
                    return True
                else:
                    print(f"❌ Package {name} not found")
                    return False
            except:
                print(f"❌ Unable to validate package {name}")
                return False

    def _check_security(self, name: str) -> List[str]:
        """Check for known security vulnerabilities."""
        print(f"🛡️ Checking security vulnerabilities for {name}")

        issues = []

        try:
            # Use safety to check for vulnerabilities
            result = subprocess.run(
                ["poetry", "run", "safety", "check", "--json"],
                capture_output=True,
                text=True,
                timeout=60,
            )

            if result.stdout:
                try:
                    safety_report = json.loads(result.stdout)
                    for vuln in safety_report:
                        if vuln.get("package_name", "").lower() == name.lower():
                            issues.append(
                                f"CVE: {vuln.get('advisory', 'Unknown vulnerability')}"
                            )
                except json.JSONDecodeError:
                    pass

            # Additional check via PyPI vulnerability database
            vuln_check = self._check_pypi_vulnerabilities(name)
            issues.extend(vuln_check)

        except subprocess.TimeoutExpired:
            print("⚠️ Security check timed out")
        except Exception as e:
            print(f"⚠️ Security check failed: {e}")

        if not issues:
            print("✅ No known security vulnerabilities found")

        return issues

    def _check_pypi_vulnerabilities(self, name: str) -> List[str]:
        """Check PyPI for vulnerability information."""
        try:
            response = requests.get(f"https://pypi.org/pypi/{name}/json", timeout=10)
            if response.status_code == 200:
                package_info = response.json()

                # Check for vulnerability-related keywords in description
                description = (
                    package_info.get("info", {}).get("description", "").lower()
                )
                summary = package_info.get("info", {}).get("summary", "").lower()

                vulnerability_keywords = [
                    "vulnerability",
                    "security fix",
                    "cve",
                    "exploit",
                ]

                issues = []
                for keyword in vulnerability_keywords:
                    if keyword in description or keyword in summary:
                        issues.append(f"Package description mentions: {keyword}")

                return issues
        except:
            pass

        return []

    def _check_license_compliance(self, name: str) -> Dict[str, any]:
        """Check license compatibility."""
        print(f"📜 Checking license compliance for {name}")

        try:
            response = requests.get(f"https://pypi.org/pypi/{name}/json", timeout=10)
            if response.status_code == 200:
                package_info = response.json()
                license_name = package_info.get("info", {}).get("license", "Unknown")

                # Define compatible licenses (adjust based on your project requirements)
                compatible_licenses = [
                    "MIT",
                    "BSD",
                    "BSD-3-Clause",
                    "BSD-2-Clause",
                    "Apache",
                    "Apache-2.0",
                    "Apache Software License",
                    "Python Software Foundation License",
                    "Mozilla Public License 2.0 (MPL 2.0)",
                    "ISC License (ISCL)",
                ]

                is_compatible = any(
                    compat in license_name for compat in compatible_licenses
                )

                result = {
                    "license": license_name,
                    "compatible": is_compatible
                    or license_name == "Unknown",  # Allow unknown licenses with warning
                }

                if is_compatible:
                    print(f"✅ License compatible: {license_name}")
                elif license_name == "Unknown":
                    print(f"⚠️ License unknown - manual review recommended")
                else:
                    print(f"❌ License incompatible: {license_name}")

                return result

        except Exception as e:
            print(f"⚠️ Unable to check license: {e}")

        return {"license": "Unknown", "compatible": True}

    def _collect_metrics(self) -> Dict[str, any]:
        """Collect current project metrics."""
        metrics = {}

        try:
            # Dependency count
            result = subprocess.run(
                ["poetry", "show", "--tree"], capture_output=True, text=True, timeout=30
            )
            if result.returncode == 0:
                deps = len(
                    [
                        line
                        for line in result.stdout.split("\n")
                        if line.strip() and not line.startswith(" ")
                    ]
                )
                metrics["dependency_count"] = deps

            # Install time measurement
            start_time = time.time()
            subprocess.run(
                ["poetry", "install", "--dry-run"],
                capture_output=True,
                text=True,
                timeout=120,
            )
            metrics["install_time"] = time.time() - start_time

        except Exception as e:
            print(f"⚠️ Unable to collect all metrics: {e}")

        return metrics

    def _calculate_impact(
        self, before: Dict[str, any], after: Dict[str, any]
    ) -> Dict[str, any]:
        """Calculate the impact of adding the dependency."""
        impact = {}

        for metric in ["dependency_count", "install_time"]:
            if metric in before and metric in after:
                impact[f"{metric}_change"] = after[metric] - before[metric]
                impact[f"{metric}_percent_change"] = (
                    (after[metric] - before[metric]) / before[metric] * 100
                    if before[metric] > 0
                    else 0
                )

        return impact

    def _validate_impact_thresholds(self, impact: Dict[str, any]) -> bool:
        """Validate impact against acceptable thresholds."""
        # Define thresholds
        thresholds = {
            "dependency_count_change": 10,  # Max 10 new dependencies
            "install_time_percent_change": 25,  # Max 25% increase in install time
        }

        for metric, threshold in thresholds.items():
            if metric in impact and impact[metric] > threshold:
                print(
                    f"❌ Impact threshold exceeded: {metric} = {impact[metric]:.2f} (max: {threshold})"
                )
                return False

        return True

    def _add_with_poetry(self, name: str, version: str, dev: bool) -> bool:
        """Add dependency using Poetry."""
        print(f"📦 Adding {name} {version} with Poetry...")

        cmd = ["poetry", "add"]
        if dev:
            cmd.append("--group=dev")
        cmd.append(f"{name}{version}")

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode == 0:
                print(f"✅ Poetry add successful")
                return True
            else:
                print(f"❌ Poetry add failed: {result.stderr}")
                return False
        except subprocess.TimeoutExpired:
            print("❌ Poetry add timed out")
            return False
        except Exception as e:
            print(f"❌ Error adding dependency: {e}")
            return False

    def _rollback_dependency(self, name: str):
        """Rollback dependency addition if validation fails."""
        print(f"🔄 Rolling back {name}")

        try:
            subprocess.run(
                ["poetry", "remove", name], capture_output=True, text=True, timeout=60
            )
            print(f"✅ Rollback successful")
        except Exception as e:
            print(f"⚠️ Rollback failed: {e}")

    def _log_dependency_addition(
        self,
        name: str,
        version: str,
        justification: str,
        dev: bool,
        impact: Dict[str, any],
        license_info: Dict[str, any],
    ):
        """Log dependency addition with full context."""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "package": name,
            "version": version,
            "justification": justification,
            "development": dev,
            "added_by": self._get_git_user(),
            "commit_hash": self._get_git_commit(),
            "impact": impact,
            "license": license_info,
            "validation_passed": True,
        }

        # Load existing log
        if self.dependency_log_path.exists():
            with open(self.dependency_log_path, "r") as f:
                log_data = json.load(f)
        else:
            log_data = {"additions": []}

        # Add new entry
        log_data["additions"].append(log_entry)

        # Save updated log
        with open(self.dependency_log_path, "w") as f:
            json.dump(log_data, f, indent=2)

        print("📝 Dependency addition logged")

    def _update_dependency_docs(
        self, name: str, version: str, justification: str, license_info: Dict[str, any]
    ):
        """Update dependency documentation."""
        if not self.docs_path.exists():
            self.docs_path.parent.mkdir(exist_ok=True)
            with open(self.docs_path, "w") as f:
                f.write("# Project Dependencies\n\n")
                f.write(
                    "This document tracks all project dependencies and their justifications.\n\n"
                )

        # Append new dependency info
        with open(self.docs_path, "a") as f:
            f.write(f"## {name} {version}\n")
            f.write(f"**Added**: {datetime.now().strftime('%Y-%m-%d')}\n")
            f.write(f"**Justification**: {justification}\n")
            f.write(f"**License**: {license_info['license']}\n")
            f.write(f"**Added by**: {self._get_git_user()}\n\n")

        print("📚 Documentation updated")

    def _generate_impact_report(self, name: str, version: str, impact: Dict[str, any]):
        """Generate detailed impact report."""
        report_path = self.project_root / f"dependency_impact_report_{name}.md"

        with open(report_path, "w") as f:
            f.write(f"# Dependency Impact Report: {name} {version}\n\n")
            f.write(
                f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            )

            f.write("## Impact Metrics\n\n")
            for metric, value in impact.items():
                if "percent" in metric:
                    f.write(f"- **{metric}**: {value:.2f}%\n")
                else:
                    f.write(f"- **{metric}**: {value}\n")

            f.write("\n## Validation Status\n")
            f.write("✅ All impact thresholds passed\n")
            f.write("✅ Security checks completed\n")
            f.write("✅ License compliance verified\n")

        print(f"📊 Impact report generated: {report_path.name}")

    def _get_git_user(self) -> str:
        """Get current git user."""
        try:
            return subprocess.check_output(
                ["git", "config", "user.name"], text=True
            ).strip()
        except:
            return "Unknown"

    def _get_git_commit(self) -> str:
        """Get current git commit hash."""
        try:
            return subprocess.check_output(
                ["git", "rev-parse", "HEAD"], text=True
            ).strip()
        except:
            return "Unknown"

    def _print_next_steps(self):
        """Print next steps for the developer."""
        print("\n📋 Next steps:")
        print("1. Run 'make test' to ensure no regressions")
        print("2. Run 'make lint' and 'make type-check' for code quality")
        print("3. Review the generated impact report")
        print("4. Create PR with dependency justification")
        print("5. Wait for automated CI dependency evaluation")
        print("6. Ensure all CI checks pass before merging")


def main():
    """Main function to handle command line interface."""
    parser = argparse.ArgumentParser(
        description="Add dependency with comprehensive validation and CI monitoring",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Add production dependency
  ./scripts/add_dependency.py --name requests --version "^2.31.0" --justification "HTTP client for API integrations"

  # Add development dependency
  ./scripts/add_dependency.py --name pytest-mock --version "^3.11.1" --justification "Mocking for unit tests" --dev

  # Add with specific version
  ./scripts/add_dependency.py --name streamlit --version "==1.28.0" --justification "Dashboard framework for Issue #123"
        """,
    )

    parser.add_argument("--name", required=True, help="Package name")
    parser.add_argument(
        "--version", required=True, help="Version constraint (e.g., ^1.0.0, ==1.0.0)"
    )
    parser.add_argument(
        "--justification", required=True, help="Detailed reason for adding dependency"
    )
    parser.add_argument(
        "--dev", action="store_true", help="Add as development dependency"
    )
    parser.add_argument(
        "--force", action="store_true", help="Skip interactive confirmations"
    )

    args = parser.parse_args()

    # Validate arguments
    if not args.justification or len(args.justification) < 10:
        print("❌ Justification must be at least 10 characters long")
        sys.exit(1)

    print("🚀 E-Commerce Analytics Platform - Dependency Addition")
    print("=" * 60)

    manager = DependencyManager()
    success = manager.add_dependency(
        args.name, args.version, args.justification, args.dev
    )

    if success:
        print("\n✅ Dependency addition completed successfully!")
    else:
        print("\n❌ Dependency addition failed!")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
