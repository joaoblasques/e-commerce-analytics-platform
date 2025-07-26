#!/bin/bash
# Install Python Dependencies for EMR Cluster
# Template for ${project_name} ${environment} environment

set -e

ENVIRONMENT="${environment}"
PROJECT_NAME="${project_name}"

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a /tmp/python-deps-install.log
}

log "Installing Python dependencies for $PROJECT_NAME-$ENVIRONMENT EMR cluster"

# Update pip
log "Updating pip..."
sudo python3 -m pip install --upgrade pip

# Install data science and analytics packages
log "Installing analytics packages..."
sudo python3 -m pip install \
    pandas==1.5.3 \
    numpy==1.24.3 \
    scipy==1.10.1 \
    scikit-learn==1.2.2 \
    matplotlib==3.7.1 \
    seaborn==0.12.2 \
    plotly==5.14.1 \
    jupyter==1.0.0 \
    jupyterlab==4.0.2

# Install AWS and cloud packages
log "Installing AWS packages..."
sudo python3 -m pip install \
    boto3==1.26.137 \
    botocore==1.29.137 \
    awscli==1.27.137 \
    s3fs==2023.5.0

# Install database connectivity packages
log "Installing database packages..."
sudo python3 -m pip install \
    psycopg2-binary==2.9.6 \
    sqlalchemy==2.0.15 \
    pymongo==4.3.3

# Install streaming and messaging packages
log "Installing streaming packages..."
sudo python3 -m pip install \
    kafka-python==2.0.2 \
    redis==4.5.5 \
    apache-airflow==2.6.1

# Install data processing packages
log "Installing data processing packages..."
sudo python3 -m pip install \
    pyarrow==12.0.0 \
    fastparquet==2023.4.0 \
    delta-spark==2.4.0 \
    pydelta==0.6.3

# Install configuration and utility packages
log "Installing utility packages..."
sudo python3 -m pip install \
    pyyaml==6.0 \
    requests==2.31.0 \
    click==8.1.3 \
    python-dateutil==2.8.2 \
    pytz==2023.3

# Install monitoring and logging packages
log "Installing monitoring packages..."
sudo python3 -m pip install \
    prometheus-client==0.16.0 \
    structlog==23.1.0 \
    python-json-logger==2.0.7

# Install machine learning packages specific to fraud detection and analytics
log "Installing ML packages..."
sudo python3 -m pip install \
    lightgbm==3.3.5 \
    xgboost==1.7.5 \
    catboost==1.2 \
    imbalanced-learn==0.10.1 \
    shap==0.41.0

# Install testing packages for development environment
if [ "$ENVIRONMENT" = "dev" ]; then
    log "Installing development and testing packages..."
    sudo python3 -m pip install \
        pytest==7.3.1 \
        pytest-cov==4.1.0 \
        black==23.3.0 \
        flake8==6.0.0 \
        mypy==1.3.0 \
        pre-commit==3.3.2
fi

# Create symbolic links for Python packages in Spark
log "Creating Spark Python package links..."
sudo ln -sf /usr/local/lib/python3.9/site-packages /usr/lib/spark/python/lib/site-packages || true

# Set up Python path for Spark
log "Configuring Python path for Spark..."
echo 'export PYTHONPATH=/usr/lib/spark/python:/usr/lib/spark/python/lib/py4j-src.zip:$PYTHONPATH' | sudo tee -a /etc/environment

# Install Jupyter kernels
log "Installing Jupyter kernels..."
sudo python3 -m ipykernel install --name python3 --display-name "Python 3.9 (EMR)"

# Set up PySpark kernel for Jupyter
log "Setting up PySpark Jupyter kernel..."
sudo mkdir -p /usr/local/share/jupyter/kernels/pyspark
cat <<EOF | sudo tee /usr/local/share/jupyter/kernels/pyspark/kernel.json
{
    "display_name": "PySpark (EMR)",
    "language": "python",
    "argv": [
        "/usr/bin/python3",
        "-m",
        "ipykernel_launcher",
        "-f",
        "{connection_file}"
    ],
    "env": {
        "SPARK_HOME": "/usr/lib/spark",
        "PYTHONPATH": "/usr/lib/spark/python:/usr/lib/spark/python/lib/py4j-src.zip",
        "PYSPARK_PYTHON": "/usr/bin/python3",
        "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3"
    }
}
EOF

# Create project-specific Python environment setup
log "Creating project environment setup..."
sudo mkdir -p /opt/ecap/python
cat <<EOF | sudo tee /opt/ecap/python/setup_env.py
#!/usr/bin/env python3
"""
Project-specific Python environment setup for $PROJECT_NAME
Environment: $ENVIRONMENT
"""

import sys
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_spark_context():
    """Setup Spark context with project-specific configuration"""
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder \
            .appName("${project_name}-${environment}-analytics") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()

        logger.info("Spark context created successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark context: {e}")
        return None

def setup_aws_credentials():
    """Setup AWS credentials for S3 access"""
    try:
        import boto3
        session = boto3.Session()
        credentials = session.get_credentials()

        if credentials:
            logger.info("AWS credentials configured successfully")
            return True
        else:
            logger.warning("No AWS credentials found")
            return False
    except Exception as e:
        logger.error(f"Failed to setup AWS credentials: {e}")
        return False

def validate_packages():
    """Validate that all required packages are installed"""
    required_packages = [
        'pandas', 'numpy', 'scipy', 'sklearn', 'boto3',
        'psycopg2', 'kafka', 'redis', 'pyarrow', 'yaml'
    ]

    missing_packages = []
    for package in required_packages:
        try:
            __import__(package)
            logger.info(f"✓ {package} is available")
        except ImportError:
            missing_packages.append(package)
            logger.error(f"✗ {package} is missing")

    if missing_packages:
        logger.error(f"Missing packages: {missing_packages}")
        return False

    logger.info("All required packages are available")
    return True

if __name__ == "__main__":
    logger.info("Setting up Python environment for $PROJECT_NAME-$ENVIRONMENT")

    # Validate packages
    if not validate_packages():
        sys.exit(1)

    # Setup AWS credentials
    setup_aws_credentials()

    # Setup Spark context
    spark = setup_spark_context()
    if spark:
        spark.stop()

    logger.info("Python environment setup completed successfully")
EOF

sudo chmod +x /opt/ecap/python/setup_env.py

# Test the installation
log "Testing Python package installation..."
python3 -c "
import pandas as pd
import numpy as np
import boto3
import pyspark
print('✓ All core packages imported successfully')
print(f'Pandas version: {pd.__version__}')
print(f'NumPy version: {np.__version__}')
print(f'PySpark version: {pyspark.__version__}')
"

# Create completion marker
log "Creating completion marker..."
echo "$(date): Python dependencies installed successfully" | sudo tee /opt/ecap/python-deps-complete

log "Python dependencies installation completed successfully for $PROJECT_NAME-$ENVIRONMENT"
exit 0
