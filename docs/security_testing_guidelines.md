# Security Testing Guidelines

## Developer Guide for Security Testing Framework

This document provides practical guidelines for developers working with the E-commerce Analytics Platform Security Testing Framework.

## Pre-Development Security Checklist

### Before Starting Development

- [ ] Review current security baselines
- [ ] Understand privacy compliance requirements
- [ ] Check for known vulnerabilities in dependencies
- [ ] Set up local security testing environment

### Setup Commands

```bash
# Install security testing dependencies
pip install safety pip-audit fastapi httpx pytest packaging

# Create initial security baseline
cd tests/security
python test_security_regression.py --create-baseline

# Validate framework installation
python test_security_framework_validation.py
```

## Development Workflow Integration

### 1. Pre-Commit Security Checks

Run before every commit:

```bash
# Quick security scan
python tests/security/test_dependency_vulnerabilities.py

# Check for personal data exposure
python tests/security/test_data_privacy_compliance.py

# Regression check
python tests/security/test_security_regression.py --continuous
```

### 2. Feature Development Security

When developing new features:

1. **API Endpoints**: Test with penetration testing module
   ```bash
   python tests/security/test_penetration_testing.py
   ```

2. **Data Handling**: Validate privacy compliance
   ```bash
   python tests/security/test_data_privacy_compliance.py
   ```

3. **Dependencies**: Check for vulnerabilities
   ```bash
   python tests/security/test_dependency_vulnerabilities.py
   ```

### 3. Code Review Security

Security considerations during code review:

- [ ] No hardcoded secrets or credentials
- [ ] Input validation and sanitization
- [ ] Proper error handling without information disclosure
- [ ] Authentication and authorization checks
- [ ] Data privacy compliance

## Common Security Patterns

### 1. Secure Coding Practices

**DO**:
```python
# Use parameterized queries
cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))

# Validate input
def validate_email(email):
    if not re.match(r'^[^@]+@[^@]+\.[^@]+$', email):
        raise ValueError("Invalid email format")

# Handle errors securely
try:
    process_data(user_input)
except Exception:
    logger.error("Data processing failed", exc_info=True)
    return {"error": "Processing failed"}
```

**DON'T**:
```python
# Never use string formatting in SQL
cursor.execute(f"SELECT * FROM users WHERE id = {user_id}")

# Don't expose internal errors
except Exception as e:
    return {"error": str(e)}  # May expose sensitive info

# Don't hardcode secrets
api_key = "sk-1234567890abcdef"  # Security violation
```

### 2. Privacy-Safe Data Handling

**Personal Data Detection Patterns**:
```python
# These patterns will be flagged by privacy compliance testing
user_email = "john@example.com"        # Email pattern
phone_number = "555-123-4567"          # Phone pattern
ssn = "123-45-6789"                    # SSN pattern

# Safe alternatives
user_email = get_from_config("user_email")
phone_number = request.validated_data.get("phone")
ssn = secure_data_handler.get_ssn(user_id)
```

### 3. Dependency Management

**Safe Dependency Practices**:
```python
# Pin versions in requirements.txt
requests>=2.31.0,<3.0.0
fastapi>=0.68.1,<1.0.0

# Regular updates
pip install --upgrade safety pip-audit
safety check
pip-audit
```

## Testing Security Features

### 1. Authentication Testing

```python
import pytest
from tests.security.test_penetration_testing import SecurityPenetrationTester

@pytest.mark.asyncio
async def test_authentication_security():
    tester = SecurityPenetrationTester("http://localhost:8000")
    vulnerabilities = await tester.test_authentication_security()
    assert len(vulnerabilities) == 0, f"Authentication vulnerabilities found: {vulnerabilities}"
```

### 2. Privacy Compliance Testing

```python
from tests.security.test_data_privacy_compliance import DataPrivacyComplianceTester

def test_privacy_compliance():
    tester = DataPrivacyComplianceTester()
    violations = tester.test_personal_data_detection()

    # Filter out test files
    real_violations = [v for v in violations if 'test' not in v['file']]
    assert len(real_violations) == 0, f"Privacy violations found: {real_violations}"
```

### 3. Regression Testing

```python
from tests.security.test_security_regression import SecurityRegressionTester

def test_no_security_regression():
    tester = SecurityRegressionTester()
    result = tester.run_continuous_regression_monitoring()
    assert result['action'] == 'proceed', f"Security regression detected: {result}"
```

## Security Issue Resolution

### 1. Dependency Vulnerabilities

When vulnerabilities are found:

1. **Identify the vulnerability**:
   ```bash
   python tests/security/test_dependency_vulnerabilities.py
   ```

2. **Check available updates**:
   ```bash
   pip list --outdated
   ```

3. **Update dependencies**:
   ```bash
   pip install --upgrade package-name
   ```

4. **Verify fix**:
   ```bash
   safety check
   pip-audit
   ```

### 2. Privacy Compliance Issues

When privacy violations are detected:

1. **Review the violation**:
   - Check if it's actual personal data or test data
   - Verify the context and necessity

2. **Remediate the issue**:
   ```python
   # Instead of hardcoded personal data
   user_email = "john@example.com"

   # Use configuration or environment variables
   user_email = os.environ.get("TEST_EMAIL", "test@example.com")
   ```

3. **Update patterns if needed**:
   - Add to safe context detection
   - Update pattern matching rules

### 3. Penetration Testing Failures

When security vulnerabilities are found:

1. **SQL Injection**: Use parameterized queries
   ```python
   # Vulnerable
   query = f"SELECT * FROM users WHERE name = '{name}'"

   # Secure
   cursor.execute("SELECT * FROM users WHERE name = %s", (name,))
   ```

2. **XSS**: Implement input sanitization
   ```python
   from html import escape

   def sanitize_input(user_input):
       return escape(user_input)
   ```

3. **Authentication Issues**: Implement proper rate limiting
   ```python
   from slowapi import Limiter, _rate_limit_exceeded_handler

   limiter = Limiter(key_func=get_remote_address)

   @app.post("/login")
   @limiter.limit("5/minute")
   async def login(request: Request, credentials: LoginCredentials):
       # Login logic
   ```

## CI/CD Integration

### 1. GitHub Actions Security Workflow

Create `.github/workflows/security.yml`:

```yaml
name: Security Testing

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  security:
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v3

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.10'

    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        pip install safety pip-audit pytest

    - name: Run Dependency Scan
      run: python tests/security/test_dependency_vulnerabilities.py

    - name: Run Privacy Compliance Check
      run: python tests/security/test_data_privacy_compliance.py

    - name: Run Security Regression Test
      run: python tests/security/test_security_regression.py --continuous

    - name: Validate Security Framework
      run: python tests/security/test_security_framework_validation.py

    - name: Run Full Security Suite
      run: pytest tests/security/ -v
```

### 2. Pre-commit Hooks

Create `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: local
    hooks:
      - id: security-dependencies
        name: Check Dependencies for Vulnerabilities
        entry: python tests/security/test_dependency_vulnerabilities.py
        language: system
        pass_filenames: false

      - id: security-privacy
        name: Check Privacy Compliance
        entry: python tests/security/test_data_privacy_compliance.py
        language: system
        pass_filenames: false

      - id: security-regression
        name: Security Regression Check
        entry: python tests/security/test_security_regression.py --continuous
        language: system
        pass_filenames: false
```

Install and run:
```bash
pip install pre-commit
pre-commit install
pre-commit run --all-files
```

## Performance Considerations

### 1. Optimization Tips

- **Parallel Testing**: Use async/await for I/O-bound operations
- **Caching**: Cache security scan results for unchanged dependencies
- **Selective Testing**: Run only relevant tests based on changed files

### 2. Resource Management

```python
# Use context managers for resources
with httpx.Client() as client:
    response = client.get(url)

# Clean up temporary files
try:
    temp_file = create_temp_file()
    process_file(temp_file)
finally:
    if temp_file.exists():
        temp_file.unlink()
```

## Monitoring and Alerting

### 1. Security Metrics

Track these metrics in your monitoring system:

- Vulnerability count by severity
- Time to remediation
- Compliance score trends
- Security test execution time

### 2. Alert Configuration

Set up alerts for:

- Critical vulnerabilities detected
- Privacy compliance violations
- Security regression incidents
- Framework validation failures

## Best Practices Summary

### Development
1. Run security tests before committing
2. Use secure coding patterns
3. Handle personal data carefully
4. Keep dependencies updated

### Testing
1. Include security tests in your test suite
2. Validate framework accuracy regularly
3. Monitor security metrics
4. Use realistic test data

### Deployment
1. Run full security suite before release
2. Update security baselines
3. Monitor for regressions
4. Have incident response procedures

### Maintenance
1. Regular dependency updates
2. Security framework updates
3. Compliance requirement reviews
4. Team security training

## Quick Reference Commands

```bash
# Daily security check
python tests/security/test_dependency_vulnerabilities.py && \
python tests/security/test_security_regression.py --continuous

# Full security audit
pytest tests/security/ -v

# Framework validation
python tests/security/test_security_framework_validation.py

# Create new baseline (after security fixes)
python tests/security/test_security_regression.py --create-baseline

# Privacy compliance check
python tests/security/test_data_privacy_compliance.py

# Penetration testing
python tests/security/test_penetration_testing.py
```

---

*These guidelines should be followed by all developers working on the E-commerce Analytics Platform. For questions or updates, please consult the security team.*
