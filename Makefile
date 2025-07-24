# E-Commerce Analytics Platform Makefile

.PHONY: help install install-dev test test-unit test-integration test-performance lint format type-check pre-commit clean build run-api run-dashboard docker-build docker-up docker-down sc-analyze sc-implement sc-improve sc-doc sc-session-start sc-session-monitor sc-quality-gates sc-help

# Default target
.DEFAULT_GOAL := help

# Colors for output
BLUE = \033[0;34m
GREEN = \033[0;32m
YELLOW = \033[1;33m
RED = \033[0;31m
NC = \033[0m # No Color

help: ## Show this help message
	@echo "$(BLUE)E-Commerce Analytics Platform - Available Commands$(NC)"
	@echo ""
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Installation
install: ## Install production dependencies
	@echo "$(BLUE)Installing production dependencies...$(NC)"
	poetry install --no-dev

install-dev: ## Install development dependencies
	@echo "$(BLUE)Installing development dependencies...$(NC)"
	poetry install
	poetry run pre-commit install

# Testing
test: ## Run all tests
	@echo "$(BLUE)Running all tests...$(NC)"
	poetry run pytest

test-unit: ## Run unit tests only
	@echo "$(BLUE)Running unit tests...$(NC)"
	poetry run pytest tests/unit/

test-integration: ## Run integration tests only
	@echo "$(BLUE)Running integration tests...$(NC)"
	poetry run pytest tests/integration/

test-performance: ## Run performance tests only
	@echo "$(BLUE)Running performance tests...$(NC)"
	poetry run pytest tests/performance/

test-coverage: ## Run tests with coverage report
	@echo "$(BLUE)Running tests with coverage...$(NC)"
	poetry run pytest --cov=src --cov-report=html --cov-report=term-missing

# Code Quality
lint: ## Run linting checks
	@echo "$(BLUE)Running linting checks...$(NC)"
	poetry run flake8 src/ tests/
	poetry run bandit -r src/

format: ## Format code with black and isort
	@echo "$(BLUE)Formatting code...$(NC)"
	poetry run black src/ tests/
	poetry run isort src/ tests/

type-check: ## Run type checking with mypy
	@echo "$(BLUE)Running type checks...$(NC)"
	poetry run mypy src/

pre-commit: ## Run pre-commit hooks on all files
	@echo "$(BLUE)Running pre-commit hooks...$(NC)"
	poetry run pre-commit run --all-files

quality: format lint type-check ## Run all code quality checks

# Development
run-api: ## Run the FastAPI server
	@echo "$(BLUE)Starting FastAPI server...$(NC)"
	poetry run uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000

run-dashboard: ## Run the Streamlit dashboard
	@echo "$(BLUE)Starting Streamlit dashboard...$(NC)"
	poetry run streamlit run src/dashboard/main.py --server.port 8501

run-spark: ## Run a Spark job example
	@echo "$(BLUE)Running Spark job example...$(NC)"
	poetry run python -m src.analytics.example_job

# Docker and Development Environment
docker-build: ## Build Docker images
	@echo "$(BLUE)Building Docker images...$(NC)"
	docker-compose build

docker-up: ## Start all services with Docker Compose
	@echo "$(BLUE)Starting services with Docker Compose...$(NC)"
	docker-compose up -d

docker-down: ## Stop all services
	@echo "$(BLUE)Stopping services...$(NC)"
	docker-compose down

docker-logs: ## View logs from all services
	@echo "$(BLUE)Viewing logs...$(NC)"
	docker-compose logs -f

# Development Environment Management
dev-start: ## Start development environment with initialization
	@echo "$(BLUE)Starting development environment...$(NC)"
	./scripts/start-dev-env.sh

dev-stop: ## Stop development environment
	@echo "$(BLUE)Stopping development environment...$(NC)"
	./scripts/stop-dev-env.sh

dev-clean: ## Clean and restart development environment
	@echo "$(BLUE)Cleaning and restarting development environment...$(NC)"
	./scripts/stop-dev-env.sh --volumes
	./scripts/start-dev-env.sh --clean

dev-status: ## Check development environment status
	@echo "$(BLUE)Checking development environment status...$(NC)"
	docker-compose ps

# Database
db-migrate: ## Run database migrations
	@echo "$(BLUE)Running database migrations...$(NC)"
	poetry run alembic upgrade head

db-reset: ## Reset database
	@echo "$(BLUE)Resetting database...$(NC)"
	poetry run alembic downgrade base
	poetry run alembic upgrade head

# Data Management
generate-data: ## Generate sample data
	@echo "$(BLUE)Generating sample data...$(NC)"
	./scripts/generate-test-data.py

generate-data-quick: ## Generate small dataset for testing
	@echo "$(BLUE)Generating quick test data...$(NC)"
	./scripts/generate-test-data.py --quick

reset-data: ## Reset all data
	@echo "$(BLUE)Resetting all data...$(NC)"
	./scripts/reset-data.sh --all --confirm

reset-data-postgres: ## Reset PostgreSQL data only
	@echo "$(BLUE)Resetting PostgreSQL data...$(NC)"
	./scripts/reset-data.sh --postgres --confirm

reset-data-kafka: ## Reset Kafka data only
	@echo "$(BLUE)Resetting Kafka data...$(NC)"
	./scripts/reset-data.sh --kafka --confirm

# Spark
spark-submit: ## Submit a Spark job
	@echo "$(BLUE)Submitting Spark job...$(NC)"
	poetry run spark-submit --master local[*] src/analytics/spark_job.py

# Kafka Management
kafka-topics: ## Create Kafka topics
	@echo "$(BLUE)Creating Kafka topics...$(NC)"
	./scripts/kafka-topics.sh create-topics

kafka-list: ## List all Kafka topics
	@echo "$(BLUE)Listing Kafka topics...$(NC)"
	./scripts/kafka-topics.sh list-topics

kafka-health: ## Check Kafka cluster health
	@echo "$(BLUE)Checking Kafka cluster health...$(NC)"
	./scripts/kafka-topics.sh health-check

kafka-reset: ## Reset all Kafka topics (DANGEROUS)
	@echo "$(BLUE)Resetting Kafka topics...$(NC)"
	./scripts/kafka-topics.sh reset-topics

# Monitoring
monitor: ## Open monitoring dashboard
	@echo "$(BLUE)Opening monitoring dashboard...$(NC)"
	open http://localhost:3000

# Cleanup
clean: ## Clean up build artifacts and cache
	@echo "$(BLUE)Cleaning up...$(NC)"
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type f -name ".coverage" -delete
	rm -rf build/
	rm -rf dist/
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .coverage
	rm -rf spark-warehouse/
	rm -rf metastore_db/

clean-docker: ## Clean up Docker resources
	@echo "$(BLUE)Cleaning Docker resources...$(NC)"
	docker-compose down --volumes --remove-orphans
	docker system prune -f

# Documentation
docs: ## Generate documentation
	@echo "$(BLUE)Generating documentation...$(NC)"
	poetry run sphinx-build -b html docs/ docs/_build/html

docs-serve: ## Serve documentation locally
	@echo "$(BLUE)Serving documentation...$(NC)"
	poetry run python -m http.server 8080 -d docs/_build/html

# Build
build: ## Build the package
	@echo "$(BLUE)Building package...$(NC)"
	poetry build

# Security
security-scan: ## Run security scan
	@echo "$(BLUE)Running security scan...$(NC)"
	poetry run bandit -r src/
	poetry run safety check

# Health Checks
health-check: ## Comprehensive health check
	@echo "$(BLUE)Running comprehensive health check...$(NC)"
	./scripts/check-health.py

test-services: ## Test core services
	@echo "$(BLUE)Testing core services...$(NC)"
	./scripts/test-services.py

test-monitoring: ## Test monitoring services
	@echo "$(BLUE)Testing monitoring services...$(NC)"
	./scripts/test-monitoring.py

health: health-check ## Alias for health-check

# Environment setup
setup-dev: install-dev ## Set up development environment
	@echo "$(BLUE)Setting up development environment...$(NC)"
	cp config/local.yaml.example config/local.yaml
	@echo "$(GREEN)Development environment set up successfully!$(NC)"
	@echo "$(YELLOW)Don't forget to:$(NC)"
	@echo "  1. Update config/local.yaml with your settings"
	@echo "  2. Run 'make docker-up' to start services"
	@echo "  3. Run 'make test' to verify everything works"

# CI/CD
ci: quality test ## Run CI pipeline locally
	@echo "$(GREEN)CI pipeline completed successfully!$(NC)"

# Version
version: ## Show version information
	@echo "$(BLUE)Version Information:$(NC)"
	@poetry version
	@echo "Python: $(shell python --version)"
	@echo "Poetry: $(shell poetry --version)"

# Database setup
setup-db: ## Set up database
	@echo "$(BLUE)Setting up database...$(NC)"
	poetry run python -m src.data.setup_db

# Full setup
setup: setup-dev docker-up setup-db ## Complete project setup
	@echo "$(GREEN)Project setup completed successfully!$(NC)"

# SuperClaude Cost-Efficient Development Commands
sc-analyze: ## SuperClaude analysis with cost optimization
	@echo "$(BLUE)Running SuperClaude analysis with cost optimization...$(NC)"
	@echo "Use: /analyze --scope module --focus [domain] --uc"
	@echo "Available domains: architecture, performance, security, quality"

sc-implement: ## SuperClaude implementation patterns
	@echo "$(BLUE)SuperClaude implementation patterns:$(NC)"
	@echo "Kafka: /implement kafka_component --framework kafka --persona-backend --uc"
	@echo "Spark: /implement spark_job --framework spark --persona-backend --c7 --uc"
	@echo "API: /implement api_endpoint --framework fastapi --persona-backend --uc"
	@echo "Dashboard: /implement dashboard_component --framework streamlit --persona-frontend --magic --uc"

sc-improve: ## SuperClaude quality improvement
	@echo "$(BLUE)Running SuperClaude quality improvement...$(NC)"
	@echo "Use: /improve --focus quality --validate --persona-refactorer --uc"

sc-doc: ## SuperClaude documentation generation
	@echo "$(BLUE)SuperClaude documentation patterns:$(NC)"
	@echo "General: /document [target] --persona-scribe --c7 --uc"
	@echo "API: /document src/api/ --focus api-docs --persona-scribe --uc"

sc-session-start: ## Start SuperClaude development session
	@echo "$(BLUE)Starting SuperClaude ECAP development session...$(NC)"
	@echo "1. Load project context: /load @ai_docs/CLAUDE.md --uc"
	@echo "2. Review current tasks: /analyze ECAP_tasklist.md --focus current-phase --uc"
	@echo "3. Initialize TODO: /todowrite session-tasks --uc"
	@echo "4. Set development focus based on current phase"

sc-session-monitor: ## Monitor SuperClaude session efficiency
	@echo "$(BLUE)SuperClaude session monitoring:$(NC)"
	@echo "Token efficiency target: 30-50% reduction"
	@echo "Always use --uc flag for cost optimization"
	@echo "Batch related questions in single session"

sc-quality-gates: ## Run SuperClaude quality validation
	@echo "$(BLUE)SuperClaude 8-step quality validation:$(NC)"
	@echo "Use: /improve implementation --focus quality --validate --uc"
	@echo "This replaces manual quality checks with automated SuperClaude validation"

sc-help: ## Show SuperClaude command patterns for ECAP
	@echo "$(BLUE)SuperClaude E-Commerce Analytics Platform Commands$(NC)"
	@echo ""
	@echo "$(GREEN)Cost-Efficient Patterns:$(NC)"
	@echo "  $(YELLOW)Always use --uc flag for 30-50% token reduction$(NC)"
	@echo "  sc-analyze         - Analysis with cost optimization"
	@echo "  sc-implement       - Framework-aware implementation patterns"
	@echo "  sc-improve         - Quality improvement with validation"
	@echo "  sc-doc             - Professional documentation generation"
	@echo ""
	@echo "$(GREEN)Session Management:$(NC)"
	@echo "  sc-session-start   - Start cost-optimized development session"
	@echo "  sc-session-monitor - Monitor token usage and efficiency"
	@echo "  sc-quality-gates   - Automated quality validation"
	@echo ""
	@echo "$(GREEN)Quick Reference:$(NC)"
	@echo "  View ai_docs/SUPERCLAUDECONFIG.md for detailed patterns"
	@echo "  View ai_docs/CLAUDE.md for complete SuperClaude integration"
