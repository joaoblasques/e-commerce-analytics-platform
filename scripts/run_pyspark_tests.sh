#!/bin/bash
# PySpark Testing Script for E-Commerce Analytics Platform
#
# This script provides multiple testing strategies for PySpark components:
# 1. Mock-based testing (no Java/Spark required)
# 2. Local Spark testing (requires Java setup)
# 3. Docker-based testing (full Spark environment)

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_PATH="$PROJECT_ROOT/src"
SPARK_VERSION="3.4.0"

# Functions
print_header() {
    echo -e "${BLUE}================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

check_java() {
    if command -v java &> /dev/null; then
        java_version=$(java -version 2>&1 | head -n 1)
        print_success "Java found: $java_version"
        return 0
    else
        print_warning "Java not found in PATH"
        return 1
    fi
}

check_spark() {
    if [ -n "$SPARK_HOME" ] && [ -d "$SPARK_HOME" ]; then
        print_success "Spark found at: $SPARK_HOME"
        return 0
    else
        print_warning "SPARK_HOME not set or directory doesn't exist"
        return 1
    fi
}

check_poetry() {
    if command -v poetry &> /dev/null; then
        print_success "Poetry found"
        return 0
    else
        print_error "Poetry not found. Please install Poetry first."
        return 1
    fi
}

setup_environment() {
    print_header "Setting up Environment"

    # Set Python path
    export PYTHONPATH="$PYTHON_PATH:$PYTHONPATH"
    export TESTING=true
    export LOG_LEVEL=ERROR

    print_success "Environment variables set"
}

run_mock_tests() {
    print_header "Running Mock-based Tests (No Spark Required)"

    cd "$PROJECT_ROOT"

    echo "Running basic data lake tests..."
    poetry run pytest tests/test_data_lake_simple.py -v --tb=short

    echo "Running improved data lake tests with mocking..."
    poetry run pytest tests/test_data_lake_improved.py -v --tb=short -m "not spark and not integration"

    echo "Running unit tests (excluding Spark)..."
    poetry run pytest tests/unit/ -v --tb=short -m "not spark" --maxfail=5

    print_success "Mock-based tests completed"
}

run_spark_validation() {
    print_header "Validating PySpark Environment"

    cd "$PROJECT_ROOT"

    echo "Testing PySpark imports..."
    poetry run python -c "
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructType, StructField, StringType
print('✅ PySpark imports successful')
"

    echo "Testing Spark session creation..."
    poetry run python -c "
from pyspark.sql import SparkSession
import tempfile

spark = SparkSession.builder \\
    .appName('TestEnvironment') \\
    .master('local[1]') \\
    .config('spark.ui.enabled', 'false') \\
    .config('spark.sql.warehouse.dir', tempfile.mkdtemp()) \\
    .getOrCreate()

data = [('Alice', 1), ('Bob', 2)]
df = spark.createDataFrame(data, ['name', 'id'])
count = df.count()
spark.stop()

print(f'✅ Spark session test successful. Row count: {count}')
"

    print_success "PySpark environment validation completed"
}

run_spark_tests() {
    print_header "Running Tests with PySpark"

    if ! check_java || ! check_spark; then
        print_warning "Java or Spark not properly configured. Setting up local Spark..."
        setup_local_spark
    fi

    cd "$PROJECT_ROOT"

    echo "Running PySpark validation tests..."
    poetry run pytest tests/test_data_lake_improved.py::TestPySparkEnvironmentValidation -v --tb=short --runspark

    echo "Running data lake tests with Spark..."
    poetry run pytest tests/test_data_lake_improved.py -v --tb=short -m "spark" --runspark

    echo "Running integration tests..."
    poetry run pytest tests/test_data_lake_improved.py -v --tb=short -m "integration" --runspark

    print_success "PySpark tests completed"
}

setup_local_spark() {
    print_header "Setting up Local Spark Environment"

    local spark_dir="/tmp/spark-$SPARK_VERSION"

    if [ ! -d "$spark_dir" ]; then
        echo "Downloading Spark $SPARK_VERSION..."
        cd /tmp
        wget -q "https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz"
        tar xzf "spark-$SPARK_VERSION-bin-hadoop3.tgz"
        mv "spark-$SPARK_VERSION-bin-hadoop3" "$spark_dir"
        rm "spark-$SPARK_VERSION-bin-hadoop3.tgz"
    fi

    export SPARK_HOME="$spark_dir"
    export PATH="$SPARK_HOME/bin:$PATH"
    export PYTHONPATH="$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"

    print_success "Local Spark environment configured at $spark_dir"
}

run_docker_tests() {
    print_header "Running Tests in Docker Environment"

    if ! command -v docker &> /dev/null; then
        print_error "Docker not found. Please install Docker first."
        return 1
    fi

    cd "$PROJECT_ROOT"

    echo "Building test environment..."
    docker-compose -f docker-compose.test.yml build test-environment

    echo "Running full PySpark tests..."
    docker-compose -f docker-compose.test.yml run --rm test-environment

    print_success "Docker-based tests completed"
}

run_coverage_report() {
    print_header "Generating Coverage Report"

    cd "$PROJECT_ROOT"

    poetry run pytest tests/ --cov=src --cov-report=html --cov-report=term-missing --tb=short -m "not spark"

    if [ -d "htmlcov" ]; then
        print_success "Coverage report generated at htmlcov/index.html"
    fi
}

install_java_macos() {
    print_header "Installing Java on macOS"

    if command -v brew &> /dev/null; then
        echo "Installing OpenJDK 11 via Homebrew..."
        brew install openjdk@11

        echo "Setting up Java environment..."
        echo 'export JAVA_HOME="/opt/homebrew/opt/openjdk@11"' >> ~/.zshrc
        echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zshrc

        export JAVA_HOME="/opt/homebrew/opt/openjdk@11"
        export PATH="$JAVA_HOME/bin:$PATH"

        print_success "Java installed. Please restart your terminal."
    else
        print_error "Homebrew not found. Please install Homebrew first or install Java manually."
        return 1
    fi
}

install_java_ubuntu() {
    print_header "Installing Java on Ubuntu/Debian"

    echo "Installing OpenJDK 11..."
    sudo apt-get update
    sudo apt-get install -y openjdk-11-jdk-headless

    echo "Setting up Java environment..."
    echo 'export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"' >> ~/.bashrc
    echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.bashrc

    export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
    export PATH="$JAVA_HOME/bin:$PATH"

    print_success "Java installed. Please restart your terminal."
}

show_help() {
    echo "PySpark Testing Script for E-Commerce Analytics Platform"
    echo ""
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  mock          Run mock-based tests (no Java/Spark required)"
    echo "  validate      Validate PySpark environment setup"
    echo "  spark         Run tests with local PySpark"
    echo "  docker        Run tests in Docker environment"
    echo "  coverage      Generate test coverage report"
    echo "  setup-spark   Setup local Spark environment"
    echo "  install-java-macos   Install Java on macOS"
    echo "  install-java-ubuntu  Install Java on Ubuntu/Debian"
    echo "  all           Run all test strategies"
    echo "  help          Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 mock               # Quick tests without Spark"
    echo "  $0 validate           # Check if PySpark works"
    echo "  $0 spark              # Full PySpark tests"
    echo "  $0 docker             # Tests in isolated environment"
    echo "  $0 all                # Run all test strategies"
}

# Main execution
main() {
    setup_environment

    case "${1:-help}" in
        "mock")
            check_poetry && run_mock_tests
            ;;
        "validate")
            check_poetry && run_spark_validation
            ;;
        "spark")
            check_poetry && run_spark_tests
            ;;
        "docker")
            run_docker_tests
            ;;
        "coverage")
            check_poetry && run_coverage_report
            ;;
        "setup-spark")
            setup_local_spark
            ;;
        "install-java-macos")
            install_java_macos
            ;;
        "install-java-ubuntu")
            install_java_ubuntu
            ;;
        "all")
            check_poetry
            run_mock_tests
            echo ""
            run_spark_validation
            echo ""
            run_spark_tests
            echo ""
            run_coverage_report
            ;;
        "help"|*)
            show_help
            ;;
    esac
}

# Execute main function
main "$@"
