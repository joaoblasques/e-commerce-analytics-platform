# Contributing to E-Commerce Analytics Platform

Thank you for your interest in contributing to the E-Commerce Analytics Platform! This document outlines our development process, coding standards, and guidelines.

## 🚀 Development Workflow

### Branch Strategy

We use a Git Flow branching strategy with the following branches:

- **`master`**: Production-ready code
- **`develop`**: Integration branch for features
- **`feature/*`**: New feature development
- **`bugfix/*`**: Bug fixes
- **`hotfix/*`**: Critical production fixes

### Branch Naming Conventions

Use the following naming patterns for your branches:

```
feature/task-description      # New features
bugfix/issue-description      # Bug fixes
hotfix/critical-issue         # Critical production fixes
docs/documentation-update     # Documentation updates
test/test-improvements        # Test-related changes
```

**Examples:**
- `feature/customer-analytics`
- `feature/fraud-detection-engine`
- `bugfix/kafka-connection-timeout`
- `hotfix/memory-leak-spark-driver`
- `docs/api-documentation`
- `test/integration-test-coverage`

### Development Process

1. **Create Feature Branch**
   ```bash
   git checkout develop
   git pull origin develop
   git checkout -b feature/your-feature-name
   ```

2. **Implement Changes**
   - Follow coding standards (see below)
   - Write tests for new functionality
   - Update documentation as needed
   - Ensure all tests pass

3. **Commit Changes**
   ```bash
   git add .
   git commit -m "feat: implement customer segmentation algorithm
   
   - Add RFM analysis for customer scoring
   - Implement cohort analysis functionality
   - Add unit tests for segmentation logic
   
   🤖 Generated with [Claude Code](https://claude.ai/code)
   
   Co-Authored-By: Claude <noreply@anthropic.com>"
   ```

4. **Push and Create Pull Request**
   ```bash
   git push origin feature/your-feature-name
   gh pr create --title "feat: implement customer segmentation" --body "Description of changes"
   ```

5. **Code Review and Merge**
   - All PRs require at least 1 approval
   - All CI checks must pass
   - Merge into `develop` branch
   - Delete feature branch after merge

## 📝 Commit Message Guidelines

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

### Format
```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types
- **feat**: New features
- **fix**: Bug fixes
- **docs**: Documentation changes
- **style**: Code style changes (formatting, etc.)
- **refactor**: Code refactoring
- **test**: Adding or updating tests
- **chore**: Maintenance tasks

### Examples
```
feat(analytics): implement customer lifetime value calculation

Add CLV calculation using historical transaction data
- Support for multiple time periods
- Integration with existing customer segmentation
- Performance optimization for large datasets

Closes #123
```

```
fix(streaming): resolve Kafka consumer lag issues

- Increase consumer fetch size
- Optimize partition assignment
- Add monitoring for consumer lag

Fixes #456
```

## 🔧 Coding Standards

### Python Code Style

We use the following tools for code quality:

- **Black**: Code formatting
- **Flake8**: Linting
- **MyPy**: Type checking
- **pytest**: Testing

### Configuration

```toml
# pyproject.toml
[tool.black]
line-length = 88
target-version = ['py39']

[tool.mypy]
python_version = "3.9"
warn_return_any = true
warn_unused_configs = true
disallow_untyped_defs = true
```

### Pre-commit Hooks

Install and use pre-commit hooks:

```bash
poetry install
poetry run pre-commit install
```

### Code Structure

```python
"""Module docstring describing the purpose."""

from typing import Optional, List, Dict
import logging

logger = logging.getLogger(__name__)


class CustomerAnalytics:
    """Class for customer analytics operations."""
    
    def __init__(self, spark_session: SparkSession) -> None:
        """Initialize with Spark session."""
        self.spark = spark_session
    
    def calculate_lifetime_value(
        self, 
        transactions_df: DataFrame,
        time_period: Optional[str] = "1Y"
    ) -> DataFrame:
        """
        Calculate customer lifetime value.
        
        Args:
            transactions_df: DataFrame with transaction data
            time_period: Time period for calculation (default: 1Y)
            
        Returns:
            DataFrame with CLV calculations
            
        Raises:
            ValueError: If time_period is invalid
        """
        # Implementation here
        pass
```

## 🧪 Testing Guidelines

### Test Structure

```
tests/
├── unit/              # Unit tests
│   ├── test_analytics.py
│   └── test_streaming.py
├── integration/       # Integration tests
│   ├── test_pipeline.py
│   └── test_kafka.py
└── performance/       # Performance tests
    └── test_spark_performance.py
```

### Test Examples

```python
import pytest
from pyspark.sql import SparkSession
from src.analytics.customer_analytics import CustomerAnalytics


class TestCustomerAnalytics:
    @pytest.fixture(scope="class")
    def spark(self):
        return SparkSession.builder \
            .appName("test") \
            .master("local[2]") \
            .getOrCreate()
    
    @pytest.fixture
    def analytics(self, spark):
        return CustomerAnalytics(spark)
    
    def test_calculate_lifetime_value(self, analytics, spark):
        # Test implementation
        test_data = [
            ("user1", "2024-01-01", 100.0),
            ("user2", "2024-01-02", 200.0)
        ]
        df = spark.createDataFrame(test_data, ["user_id", "date", "amount"])
        
        result = analytics.calculate_lifetime_value(df)
        
        assert result.count() == 2
        assert "clv" in result.columns
```

### Running Tests

```bash
# Run all tests
poetry run pytest

# Run specific test categories
poetry run pytest tests/unit/
poetry run pytest tests/integration/

# Run with coverage
poetry run pytest --cov=src --cov-report=html
```

## 📊 Performance Guidelines

### Spark Best Practices

1. **DataFrame Operations**
   - Use DataFrame API instead of RDDs
   - Avoid collect() on large datasets
   - Use appropriate partitioning

2. **Memory Management**
   - Configure Spark memory settings
   - Use broadcast joins for small lookup tables
   - Cache frequently accessed DataFrames

3. **Optimization**
   - Use appropriate storage levels
   - Optimize join strategies
   - Monitor Spark UI for performance bottlenecks

### Example Optimizations

```python
# Good: Use broadcast for small lookup tables
broadcast_products = broadcast(products_df)
enriched_df = transactions_df.join(broadcast_products, "product_id")

# Good: Appropriate partitioning
partitioned_df = transactions_df.repartition(col("date"))

# Good: Caching frequently used DataFrames
customer_df.cache()
```

## 🔒 Security Guidelines

### Data Security

- Never commit secrets or credentials
- Use environment variables for configuration
- Implement proper data masking
- Follow data privacy regulations

### Code Security

- Validate all inputs
- Use parameterized queries
- Implement proper error handling
- Follow secure coding practices

## 📖 Documentation

### Code Documentation

- Write clear docstrings for all functions and classes
- Include type hints
- Document complex algorithms
- Provide usage examples

### Project Documentation

- Update README.md for major changes
- Document architectural decisions
- Maintain API documentation
- Create troubleshooting guides

## 🤝 Code Review Process

### Review Checklist

- [ ] Code follows style guidelines
- [ ] All tests pass
- [ ] Documentation is updated
- [ ] Performance considerations addressed
- [ ] Security best practices followed
- [ ] No hardcoded secrets or credentials

### Review Guidelines

- Be constructive and respectful
- Focus on code quality and maintainability
- Suggest improvements with explanations
- Approve only when confident in code quality

## 🚀 Deployment

### Development Environment

```bash
# Set up local development
docker-compose up -d
poetry install
poetry run pytest
```

### Production Deployment

- All changes must go through `develop` branch
- Production deployments from `master` branch only
- CI/CD pipeline must pass all checks
- Monitoring and alerting configured

## 📞 Getting Help

- Create GitHub issues for bugs or feature requests
- Use GitHub Discussions for questions
- Check existing documentation first
- Follow issue templates when reporting bugs

## 🎯 Quality Gates

All contributions must meet these requirements:

- [ ] Code coverage > 90%
- [ ] All tests pass
- [ ] Code style checks pass
- [ ] Documentation updated
- [ ] Performance tests pass
- [ ] Security scan passes

Thank you for contributing to the E-Commerce Analytics Platform!