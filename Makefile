# E-Commerce Analytics Platform Makefile

.PHONY: help install install-dev test test-unit test-integration test-performance lint format type-check pre-commit clean build run-api run-dashboard docker-build docker-up docker-down

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

# Docker
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

# Database
db-migrate: ## Run database migrations
	@echo "$(BLUE)Running database migrations...$(NC)"
	poetry run alembic upgrade head

db-reset: ## Reset database
	@echo "$(BLUE)Resetting database...$(NC)"
	poetry run alembic downgrade base
	poetry run alembic upgrade head

# Data
generate-data: ## Generate sample data
	@echo "$(BLUE)Generating sample data...$(NC)"
	poetry run python -m src.data.generator

# Spark
spark-submit: ## Submit a Spark job
	@echo "$(BLUE)Submitting Spark job...$(NC)"
	poetry run spark-submit --master local[*] src/analytics/spark_job.py

# Kafka
kafka-topics: ## Create Kafka topics
	@echo "$(BLUE)Creating Kafka topics...$(NC)"
	poetry run python -m src.streaming.setup_topics

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

# Health check
health: ## Check system health
	@echo "$(BLUE)Checking system health...$(NC)"
	poetry run python -m src.utils.health_check

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