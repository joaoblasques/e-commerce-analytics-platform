"""
Security regression testing framework.

This module provides automated security regression testing to ensure
that security fixes remain effective and new vulnerabilities are not introduced.
"""

import hashlib
import json
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pytest


class SecurityRegressionTester:
    """Automated security regression testing framework."""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.regression_db = (
            self.project_root / "tests" / "security" / "security_regression.db"
        )
        self.baseline_file = (
            self.project_root / "tests" / "security" / "security_baseline.json"
        )
        self._init_regression_database()

    def _init_regression_database(self):
        """Initialize the security regression database."""
        self.regression_db.parent.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(self.regression_db) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS security_baselines (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    test_type TEXT NOT NULL,
                    test_name TEXT NOT NULL,
                    baseline_hash TEXT NOT NULL,
                    baseline_data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(test_type, test_name)
                )
            """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS security_test_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    test_type TEXT NOT NULL,
                    test_name TEXT NOT NULL,
                    result_hash TEXT NOT NULL,
                    result_data TEXT NOT NULL,
                    status TEXT NOT NULL,
                    run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS regression_incidents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    test_type TEXT NOT NULL,
                    test_name TEXT NOT NULL,
                    incident_type TEXT NOT NULL,
                    description TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resolved_at TIMESTAMP NULL,
                    resolution TEXT NULL
                )
            """
            )

    def create_security_baseline(self) -> Dict:
        """Create security baseline for regression testing."""
        print("📊 Creating security baseline...")

        baseline_data = {
            "created_at": datetime.now().isoformat(),
            "code_hash": self._calculate_codebase_hash(),
            "dependency_scan": self._run_dependency_baseline(),
            "static_analysis": self._run_static_analysis_baseline(),
            "configuration_scan": self._run_configuration_baseline(),
            "secret_scan": self._run_secret_scan_baseline(),
            "permission_scan": self._run_permission_baseline(),
        }

        # Store baseline in database
        self._store_baseline("comprehensive", "security_baseline", baseline_data)

        # Also save to file for version control
        with open(self.baseline_file, "w") as f:
            json.dump(baseline_data, f, indent=2)

        return baseline_data

    def _calculate_codebase_hash(self) -> str:
        """Calculate hash of relevant codebase files."""
        relevant_files = []

        # Collect source files
        for pattern in ["**/*.py", "**/*.yaml", "**/*.yml", "**/*.json", "**/*.sql"]:
            relevant_files.extend(self.project_root.glob(pattern))

        # Filter out unimportant files and directories
        filtered_files = [
            f
            for f in relevant_files
            if f.is_file()
            and not any(
                exclude in str(f)
                for exclude in [
                    "__pycache__",
                    ".git",
                    ".pytest_cache",
                    "node_modules",
                    ".venv",
                    "venv",
                    "htmlcov",
                    ".coverage",
                ]
            )
        ]

        # Sort for consistent hashing
        filtered_files.sort()

        # Calculate combined hash
        hasher = hashlib.sha256()
        for file_path in filtered_files:
            try:
                # Skip if it's a directory
                if file_path.is_dir():
                    continue
                with open(file_path, "rb") as f:
                    hasher.update(f.read())
            except (PermissionError, FileNotFoundError, IsADirectoryError):
                continue

        return hasher.hexdigest()

    def _run_dependency_baseline(self) -> Dict:
        """Run dependency security baseline."""
        try:
            # Run Safety check
            result = subprocess.run(
                [sys.executable, "-m", "safety", "check", "--json"],
                capture_output=True,
                text=True,
                cwd=self.project_root,
            )

            if result.returncode == 0:
                vulnerabilities = []
            else:
                try:
                    vulnerabilities = json.loads(result.stdout)
                except json.JSONDecodeError:
                    vulnerabilities = []

            return {
                "tool": "safety",
                "vulnerabilities_count": len(vulnerabilities),
                "vulnerabilities": vulnerabilities,
                "status": "clean" if len(vulnerabilities) == 0 else "issues_found",
            }

        except Exception as e:
            return {"tool": "safety", "error": str(e), "status": "error"}

    def _run_static_analysis_baseline(self) -> Dict:
        """Run static analysis security baseline."""
        try:
            # Run Bandit
            result = subprocess.run(
                [sys.executable, "-m", "bandit", "-r", "src/", "-f", "json"],
                capture_output=True,
                text=True,
                cwd=self.project_root,
            )

            try:
                bandit_results = json.loads(result.stdout)
            except json.JSONDecodeError:
                bandit_results = {"results": [], "errors": []}

            return {
                "tool": "bandit",
                "issues_count": len(bandit_results.get("results", [])),
                "results": bandit_results.get("results", []),
                "errors": bandit_results.get("errors", []),
                "status": "clean"
                if len(bandit_results.get("results", [])) == 0
                else "issues_found",
            }

        except Exception as e:
            return {"tool": "bandit", "error": str(e), "status": "error"}

    def _run_configuration_baseline(self) -> Dict:
        """Run configuration security baseline."""
        config_issues = []

        # Check for insecure configurations
        config_files = [f for f in self.project_root.glob("**/*.yaml") if f.is_file()]
        config_files.extend(
            [f for f in self.project_root.glob("**/*.yml") if f.is_file()]
        )
        config_files.extend(
            [f for f in self.project_root.glob("**/*.json") if f.is_file()]
        )

        insecure_patterns = [
            "debug: true",
            "debug=true",
            "ssl_verify: false",
            "verify_ssl: false",
            "insecure: true",
        ]

        for config_file in config_files:
            try:
                # Skip if it's a directory
                if config_file.is_dir():
                    continue

                with open(config_file, "r", encoding="utf-8") as f:
                    content = f.read().lower()

                for pattern in insecure_patterns:
                    if pattern.lower() in content:
                        config_issues.append(
                            {
                                "file": str(config_file.relative_to(self.project_root)),
                                "issue": pattern,
                                "severity": "medium",
                            }
                        )

            except (UnicodeDecodeError, PermissionError, IsADirectoryError):
                continue

        return {
            "tool": "config_scan",
            "issues_count": len(config_issues),
            "issues": config_issues,
            "status": "clean" if len(config_issues) == 0 else "issues_found",
        }

    def _run_secret_scan_baseline(self) -> Dict:
        """Run secret scanning baseline."""
        secret_patterns = {
            "api_key": r"api[_-]?key['\"]?\s*[:=]\s*['\"]?[a-zA-Z0-9]{20,}",
            "password": r"password['\"]?\s*[:=]\s*['\"]?[^'\"\s]{8,}",
            "token": r"token['\"]?\s*[:=]\s*['\"]?[a-zA-Z0-9]{20,}",
            "secret": r"secret['\"]?\s*[:=]\s*['\"]?[a-zA-Z0-9]{16,}",
            "private_key": r"-----BEGIN (RSA )?PRIVATE KEY-----",
        }

        secrets_found = []
        source_files = [f for f in self.project_root.glob("**/*.py") if f.is_file()]
        source_files.extend(
            [f for f in self.project_root.glob("**/*.yaml") if f.is_file()]
        )
        source_files.extend(
            [f for f in self.project_root.glob("**/*.yml") if f.is_file()]
        )
        source_files.extend(
            [f for f in self.project_root.glob("**/*.json") if f.is_file()]
        )

        import re

        for file_path in source_files:
            try:
                # Skip if it's a directory
                if file_path.is_dir():
                    continue

                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()

                for secret_type, pattern in secret_patterns.items():
                    matches = re.finditer(pattern, content, re.IGNORECASE)
                    for match in matches:
                        # Skip test files and comments
                        if self._is_test_or_comment_context(content, match.start()):
                            continue

                        secrets_found.append(
                            {
                                "type": secret_type,
                                "file": str(file_path.relative_to(self.project_root)),
                                "line": content[: match.start()].count("\n") + 1,
                                "severity": "high",
                            }
                        )

            except (UnicodeDecodeError, PermissionError, IsADirectoryError):
                continue

        return {
            "tool": "secret_scan",
            "secrets_count": len(secrets_found),
            "secrets": secrets_found,
            "status": "clean" if len(secrets_found) == 0 else "issues_found",
        }

    def _is_test_or_comment_context(self, content: str, position: int) -> bool:
        """Check if position is in test or comment context."""
        lines = content[:position].split("\n")
        current_line = lines[-1] if lines else ""

        # Check if in comment
        if "#" in current_line:
            return True

        # Check if in test file or test context
        test_indicators = ["test_", "mock_", "fake_", "example_", "dummy_"]
        line_lower = current_line.lower()

        return any(indicator in line_lower for indicator in test_indicators)

    def _run_permission_baseline(self) -> Dict:
        """Run file permission baseline."""
        permission_issues = []

        # Check for overly permissive files
        for file_path in self.project_root.rglob("*"):
            if file_path.is_file():
                try:
                    stat = file_path.stat()
                    # Convert to octal permissions
                    perms = oct(stat.st_mode)[-3:]

                    # Check for world-writable files
                    if perms.endswith("6") or perms.endswith("7"):
                        permission_issues.append(
                            {
                                "file": str(file_path.relative_to(self.project_root)),
                                "permissions": perms,
                                "issue": "world_writable",
                                "severity": "medium",
                            }
                        )

                except OSError:
                    continue

        return {
            "tool": "permission_scan",
            "issues_count": len(permission_issues),
            "issues": permission_issues,
            "status": "clean" if len(permission_issues) == 0 else "issues_found",
        }

    def _store_baseline(self, test_type: str, test_name: str, baseline_data: Dict):
        """Store baseline in database."""
        baseline_json = json.dumps(baseline_data, sort_keys=True)
        baseline_hash = hashlib.sha256(baseline_json.encode()).hexdigest()

        with sqlite3.connect(self.regression_db) as conn:
            conn.execute(
                """INSERT OR REPLACE INTO security_baselines
                   (test_type, test_name, baseline_hash, baseline_data)
                   VALUES (?, ?, ?, ?)""",
                (test_type, test_name, baseline_hash, baseline_json),
            )

    def run_regression_test(self) -> Dict:
        """Run security regression test against baseline."""
        print("🔍 Running security regression test...")

        # Get current security state
        current_state = {
            "tested_at": datetime.now().isoformat(),
            "code_hash": self._calculate_codebase_hash(),
            "dependency_scan": self._run_dependency_baseline(),
            "static_analysis": self._run_static_analysis_baseline(),
            "configuration_scan": self._run_configuration_baseline(),
            "secret_scan": self._run_secret_scan_baseline(),
            "permission_scan": self._run_permission_baseline(),
        }

        # Load baseline
        baseline = self._load_baseline("comprehensive", "security_baseline")

        if not baseline:
            return {
                "status": "no_baseline",
                "message": "No security baseline found. Run create_security_baseline() first.",
                "current_state": current_state,
            }

        # Compare against baseline
        regression_results = self._compare_against_baseline(baseline, current_state)

        # Store test results
        self._store_test_results(
            "regression", "comprehensive", current_state, regression_results
        )

        return regression_results

    def _load_baseline(self, test_type: str, test_name: str) -> Optional[Dict]:
        """Load baseline from database."""
        with sqlite3.connect(self.regression_db) as conn:
            cursor = conn.execute(
                """SELECT baseline_data FROM security_baselines
                   WHERE test_type = ? AND test_name = ?
                   ORDER BY created_at DESC LIMIT 1""",
                (test_type, test_name),
            )
            row = cursor.fetchone()

            if row:
                return json.loads(row[0])
            return None

    def _compare_against_baseline(self, baseline: Dict, current: Dict) -> Dict:
        """Compare current state against baseline."""
        regressions = []
        improvements = []

        # Compare dependency vulnerabilities
        baseline_deps = baseline.get("dependency_scan", {}).get(
            "vulnerabilities_count", 0
        )
        current_deps = current.get("dependency_scan", {}).get(
            "vulnerabilities_count", 0
        )

        if current_deps > baseline_deps:
            regressions.append(
                {
                    "type": "dependency_regression",
                    "severity": "high",
                    "description": f"Dependency vulnerabilities increased from {baseline_deps} to {current_deps}",
                    "baseline_count": baseline_deps,
                    "current_count": current_deps,
                }
            )
        elif current_deps < baseline_deps:
            improvements.append(
                {
                    "type": "dependency_improvement",
                    "description": f"Dependency vulnerabilities decreased from {baseline_deps} to {current_deps}",
                }
            )

        # Compare static analysis issues
        baseline_static = baseline.get("static_analysis", {}).get("issues_count", 0)
        current_static = current.get("static_analysis", {}).get("issues_count", 0)

        if current_static > baseline_static:
            regressions.append(
                {
                    "type": "static_analysis_regression",
                    "severity": "medium",
                    "description": f"Static analysis issues increased from {baseline_static} to {current_static}",
                    "baseline_count": baseline_static,
                    "current_count": current_static,
                }
            )
        elif current_static < baseline_static:
            improvements.append(
                {
                    "type": "static_analysis_improvement",
                    "description": f"Static analysis issues decreased from {baseline_static} to {current_static}",
                }
            )

        # Compare configuration issues
        baseline_config = baseline.get("configuration_scan", {}).get("issues_count", 0)
        current_config = current.get("configuration_scan", {}).get("issues_count", 0)

        if current_config > baseline_config:
            regressions.append(
                {
                    "type": "configuration_regression",
                    "severity": "medium",
                    "description": f"Configuration issues increased from {baseline_config} to {current_config}",
                    "baseline_count": baseline_config,
                    "current_count": current_config,
                }
            )

        # Compare secrets
        baseline_secrets = baseline.get("secret_scan", {}).get("secrets_count", 0)
        current_secrets = current.get("secret_scan", {}).get("secrets_count", 0)

        if current_secrets > baseline_secrets:
            regressions.append(
                {
                    "type": "secret_regression",
                    "severity": "critical",
                    "description": f"Exposed secrets increased from {baseline_secrets} to {current_secrets}",
                    "baseline_count": baseline_secrets,
                    "current_count": current_secrets,
                }
            )

        # Compare permissions
        baseline_perms = baseline.get("permission_scan", {}).get("issues_count", 0)
        current_perms = current.get("permission_scan", {}).get("issues_count", 0)

        if current_perms > baseline_perms:
            regressions.append(
                {
                    "type": "permission_regression",
                    "severity": "low",
                    "description": f"Permission issues increased from {baseline_perms} to {current_perms}",
                    "baseline_count": baseline_perms,
                    "current_count": current_perms,
                }
            )

        # Calculate overall status
        if regressions:
            # Check for critical regressions
            critical_regressions = [
                r for r in regressions if r["severity"] == "critical"
            ]
            high_regressions = [r for r in regressions if r["severity"] == "high"]

            if critical_regressions:
                status = "critical_regression"
            elif high_regressions:
                status = "high_regression"
            else:
                status = "regression_detected"
        else:
            status = "no_regression"

        return {
            "status": status,
            "tested_at": current["tested_at"],
            "regressions": regressions,
            "improvements": improvements,
            "regression_count": len(regressions),
            "improvement_count": len(improvements),
            "baseline_date": baseline.get("created_at"),
            "recommendations": self._generate_regression_recommendations(regressions),
        }

    def _store_test_results(
        self, test_type: str, test_name: str, test_data: Dict, result: Dict
    ):
        """Store test results in database."""
        result_json = json.dumps(result, sort_keys=True)
        result_hash = hashlib.sha256(result_json.encode()).hexdigest()

        with sqlite3.connect(self.regression_db) as conn:
            conn.execute(
                """INSERT INTO security_test_results
                   (test_type, test_name, result_hash, result_data, status)
                   VALUES (?, ?, ?, ?, ?)""",
                (test_type, test_name, result_hash, result_json, result["status"]),
            )

            # Store regression incidents
            for regression in result.get("regressions", []):
                conn.execute(
                    """INSERT INTO regression_incidents
                       (test_type, test_name, incident_type, description, severity)
                       VALUES (?, ?, ?, ?, ?)""",
                    (
                        test_type,
                        test_name,
                        regression["type"],
                        regression["description"],
                        regression["severity"],
                    ),
                )

    def _generate_regression_recommendations(
        self, regressions: List[Dict]
    ) -> List[str]:
        """Generate recommendations for fixing regressions."""
        if not regressions:
            return ["✅ No security regressions detected"]

        recommendations = []

        regression_types = {r["type"] for r in regressions}

        if "dependency_regression" in regression_types:
            recommendations.append("📦 Update dependencies to fix new vulnerabilities")

        if "static_analysis_regression" in regression_types:
            recommendations.append(
                "🔍 Review and fix new static analysis security issues"
            )

        if "secret_regression" in regression_types:
            recommendations.append(
                "🔑 Remove exposed secrets and implement secret management"
            )

        if "configuration_regression" in regression_types:
            recommendations.append("⚙️ Review and secure configuration settings")

        if "permission_regression" in regression_types:
            recommendations.append("🔒 Fix file permission issues")

        recommendations.extend(
            [
                "🔄 Run security scans before each release",
                "📋 Establish security review process for code changes",
                "🎯 Set up automated regression testing in CI/CD",
                "📊 Monitor security metrics over time",
                "🛠️ Integrate security tools into development workflow",
            ]
        )

        return recommendations

    def get_regression_history(self, days: int = 30) -> Dict:
        """Get regression test history."""
        with sqlite3.connect(self.regression_db) as conn:
            cursor = conn.execute(
                """SELECT * FROM security_test_results
                   WHERE test_type = 'regression'
                   AND run_at > datetime('now', '-{} days')
                   ORDER BY run_at DESC""".format(
                    days
                )
            )

            results = []
            for row in cursor.fetchall():
                results.append(
                    {
                        "id": row[0],
                        "test_name": row[2],
                        "status": row[5],
                        "run_at": row[6],
                        "result_data": json.loads(row[4]),
                    }
                )

            return {
                "history_period_days": days,
                "total_runs": len(results),
                "results": results,
            }

    def run_continuous_regression_monitoring(self) -> Dict:
        """Run continuous regression monitoring suitable for CI/CD."""
        print("🔄 Running continuous security regression monitoring...")

        # Quick regression check focusing on critical issues
        current_critical_state = {
            "dependency_scan": self._run_dependency_baseline(),
            "secret_scan": self._run_secret_scan_baseline(),
        }

        # Load baseline
        baseline = self._load_baseline("comprehensive", "security_baseline")

        if not baseline:
            # If no baseline, create one and pass
            self.create_security_baseline()
            return {
                "status": "baseline_created",
                "message": "Created new security baseline",
                "action": "proceed",
            }

        # Check for critical regressions only
        critical_regressions = []

        # Check dependencies
        baseline_deps = baseline.get("dependency_scan", {}).get(
            "vulnerabilities_count", 0
        )
        current_deps = current_critical_state.get("dependency_scan", {}).get(
            "vulnerabilities_count", 0
        )

        if current_deps > baseline_deps:
            critical_regressions.append(
                {
                    "type": "dependency_regression",
                    "severity": "high",
                    "description": f"New dependency vulnerabilities: {current_deps - baseline_deps}",
                }
            )

        # Check secrets
        baseline_secrets = baseline.get("secret_scan", {}).get("secrets_count", 0)
        current_secrets = current_critical_state.get("secret_scan", {}).get(
            "secrets_count", 0
        )

        if current_secrets > baseline_secrets:
            critical_regressions.append(
                {
                    "type": "secret_regression",
                    "severity": "critical",
                    "description": f"New exposed secrets: {current_secrets - baseline_secrets}",
                }
            )

        if critical_regressions:
            return {
                "status": "critical_regression",
                "regressions": critical_regressions,
                "action": "block_deployment",
                "message": "Critical security regressions detected. Deployment should be blocked.",
            }
        else:
            return {
                "status": "no_critical_regression",
                "action": "proceed",
                "message": "No critical security regressions detected",
            }


class TestSecurityRegression:
    """Test cases for security regression testing."""

    @pytest.fixture
    def tester(self):
        """Create security regression tester instance."""
        return SecurityRegressionTester()

    def test_regression_database_initialization(self, tester):
        """Test regression database initialization."""
        # Database should be created and initialized
        assert tester.regression_db.exists()

        with sqlite3.connect(tester.regression_db) as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]

            expected_tables = [
                "security_baselines",
                "security_test_results",
                "regression_incidents",
            ]

            for table in expected_tables:
                assert table in tables

    def test_codebase_hash_calculation(self, tester):
        """Test codebase hash calculation."""
        hash1 = tester._calculate_codebase_hash()
        hash2 = tester._calculate_codebase_hash()

        # Hash should be consistent
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA256 hex string length

    def test_dependency_baseline_creation(self, tester):
        """Test dependency security baseline creation."""
        result = tester._run_dependency_baseline()

        assert "tool" in result
        assert "status" in result
        assert result["tool"] == "safety"
        assert result["status"] in ["clean", "issues_found", "error"]

    def test_static_analysis_baseline_creation(self, tester):
        """Test static analysis baseline creation."""
        result = tester._run_static_analysis_baseline()

        assert "tool" in result
        assert "status" in result
        assert result["tool"] == "bandit"
        assert result["status"] in ["clean", "issues_found", "error"]

    def test_secret_scanning_baseline(self, tester):
        """Test secret scanning baseline."""
        result = tester._run_secret_scan_baseline()

        assert "tool" in result
        assert "secrets_count" in result
        assert "status" in result
        assert isinstance(result["secrets_count"], int)

    def test_configuration_scanning_baseline(self, tester):
        """Test configuration scanning baseline."""
        result = tester._run_configuration_baseline()

        assert "tool" in result
        assert "issues_count" in result
        assert "status" in result
        assert isinstance(result["issues_count"], int)

    def test_permission_scanning_baseline(self, tester):
        """Test permission scanning baseline."""
        result = tester._run_permission_baseline()

        assert "tool" in result
        assert "issues_count" in result
        assert "status" in result
        assert isinstance(result["issues_count"], int)

    def test_baseline_storage_and_retrieval(self, tester):
        """Test baseline storage and retrieval."""
        test_baseline = {"test": "data", "created_at": datetime.now().isoformat()}

        # Store baseline
        tester._store_baseline("test", "test_baseline", test_baseline)

        # Retrieve baseline
        retrieved = tester._load_baseline("test", "test_baseline")

        assert retrieved is not None
        assert retrieved["test"] == "data"

    def test_security_baseline_creation(self, tester):
        """Test comprehensive security baseline creation."""
        baseline = tester.create_security_baseline()

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

    def test_regression_comparison(self, tester):
        """Test regression comparison logic."""
        # Create mock baseline and current state
        baseline = {
            "dependency_scan": {"vulnerabilities_count": 2},
            "static_analysis": {"issues_count": 5},
            "secret_scan": {"secrets_count": 0},
        }

        current = {
            "dependency_scan": {"vulnerabilities_count": 3},  # Regression
            "static_analysis": {"issues_count": 3},  # Improvement
            "secret_scan": {"secrets_count": 1},  # Critical regression
            "tested_at": datetime.now().isoformat(),
        }

        result = tester._compare_against_baseline(baseline, current)

        assert "status" in result
        assert "regressions" in result
        assert "improvements" in result

        # Should detect regressions
        assert len(result["regressions"]) > 0

        # Should detect critical regression due to secrets
        assert result["status"] == "critical_regression"

    def test_continuous_monitoring(self, tester):
        """Test continuous regression monitoring."""
        # Create baseline first
        tester.create_security_baseline()

        # Run continuous monitoring
        result = tester.run_continuous_regression_monitoring()

        assert "status" in result
        assert "action" in result
        assert result["action"] in ["proceed", "block_deployment"]

    def test_regression_recommendations(self, tester):
        """Test regression recommendation generation."""
        mock_regressions = [
            {"type": "dependency_regression", "severity": "high"},
            {"type": "secret_regression", "severity": "critical"},
        ]

        recommendations = tester._generate_regression_recommendations(mock_regressions)

        assert isinstance(recommendations, list)
        assert len(recommendations) > 0
        assert any("dependencies" in rec.lower() for rec in recommendations)
        assert any("secret" in rec.lower() for rec in recommendations)

    def test_regression_history_retrieval(self, tester):
        """Test regression history retrieval."""
        history = tester.get_regression_history(days=7)

        assert "history_period_days" in history
        assert "total_runs" in history
        assert "results" in history
        assert history["history_period_days"] == 7

    def test_test_context_detection(self, tester):
        """Test detection of test/comment context."""
        # Test with comment
        content_with_comment = "# This is a test_password = 'secret123'"
        assert tester._is_test_or_comment_context(content_with_comment, 25)

        # Test with test context
        content_with_test = "mock_api_key = 'fake_key_12345'"
        assert tester._is_test_or_comment_context(content_with_test, 10)

        # Test with normal code
        content_normal = "real_password = get_password()"
        assert not tester._is_test_or_comment_context(content_normal, 10)


if __name__ == "__main__":
    # Command-line execution for security regression testing
    import argparse

    parser = argparse.ArgumentParser(description="Security Regression Testing")
    parser.add_argument(
        "--create-baseline", action="store_true", help="Create security baseline"
    )
    parser.add_argument("--run-test", action="store_true", help="Run regression test")
    parser.add_argument(
        "--continuous", action="store_true", help="Run continuous monitoring"
    )
    parser.add_argument(
        "--history", type=int, default=30, help="Get history for N days"
    )

    args = parser.parse_args()

    tester = SecurityRegressionTester()

    if args.create_baseline:
        print("Creating security baseline...")
        baseline = tester.create_security_baseline()
        print(f"✅ Baseline created with {len(baseline)} components")

    elif args.run_test:
        print("Running security regression test...")
        result = tester.run_regression_test()
        print(f"📊 Test Status: {result['status']}")
        if result.get("regressions"):
            print(f"⚠️ Regressions found: {len(result['regressions'])}")
            for reg in result["regressions"]:
                print(f"  - {reg['type']}: {reg['description']}")

    elif args.continuous:
        print("Running continuous monitoring...")
        result = tester.run_continuous_regression_monitoring()
        print(f"🔄 Status: {result['status']}")
        print(f"🎯 Action: {result['action']}")
        if result.get("regressions"):
            print("⚠️ Critical regressions detected!")
            for reg in result["regressions"]:
                print(f"  - {reg['description']}")

    else:
        print("Getting regression history...")
        history = tester.get_regression_history(args.history)
        print(f"📈 History ({args.history} days): {history['total_runs']} runs")
        for result in history["results"][:5]:  # Show last 5
            print(f"  - {result['run_at']}: {result['status']}")
