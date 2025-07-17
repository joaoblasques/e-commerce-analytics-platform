#!/bin/bash
# MinIO Bucket Initialization Script
# This script creates the necessary buckets for the e-commerce analytics platform

set -e

# MinIO server details
MINIO_HOST="http://minio:9000"
MINIO_ALIAS="ecap-minio"

# Wait for MinIO to be ready
echo "Waiting for MinIO to be ready..."
until mc ping "$MINIO_ALIAS" > /dev/null 2>&1; do
    echo "MinIO not ready yet, waiting 5 seconds..."
    sleep 5
done

echo "MinIO is ready! Creating buckets..."

# Create buckets for different data types
BUCKETS=(
    "raw-data"
    "processed-data"
    "analytics-results"
    "model-artifacts"
    "logs"
    "backups"
)

for bucket in "${BUCKETS[@]}"; do
    echo "Creating bucket: $bucket"
    mc mb "$MINIO_ALIAS/$bucket" --ignore-existing
    
    # Set bucket policy to allow read/write access
    mc anonymous set download "$MINIO_ALIAS/$bucket"
    echo "Created bucket: $bucket"
done

# Create folder structure in buckets
echo "Creating folder structure..."

# Raw data folders
mc put /dev/null "$MINIO_ALIAS/raw-data/transactions/"
mc put /dev/null "$MINIO_ALIAS/raw-data/user-events/"
mc put /dev/null "$MINIO_ALIAS/raw-data/product-catalog/"
mc put /dev/null "$MINIO_ALIAS/raw-data/customer-data/"

# Processed data folders
mc put /dev/null "$MINIO_ALIAS/processed-data/daily-aggregates/"
mc put /dev/null "$MINIO_ALIAS/processed-data/customer-segments/"
mc put /dev/null "$MINIO_ALIAS/processed-data/fraud-scores/"

# Analytics results folders
mc put /dev/null "$MINIO_ALIAS/analytics-results/reports/"
mc put /dev/null "$MINIO_ALIAS/analytics-results/dashboards/"
mc put /dev/null "$MINIO_ALIAS/analytics-results/exports/"

# Model artifacts folders
mc put /dev/null "$MINIO_ALIAS/model-artifacts/fraud-detection/"
mc put /dev/null "$MINIO_ALIAS/model-artifacts/recommendation/"
mc put /dev/null "$MINIO_ALIAS/model-artifacts/segmentation/"

# Logs folders
mc put /dev/null "$MINIO_ALIAS/logs/spark-jobs/"
mc put /dev/null "$MINIO_ALIAS/logs/kafka-streams/"
mc put /dev/null "$MINIO_ALIAS/logs/api-access/"

# Backups folders
mc put /dev/null "$MINIO_ALIAS/backups/database/"
mc put /dev/null "$MINIO_ALIAS/backups/configurations/"

echo "MinIO bucket initialization completed successfully!"
echo "Available buckets:"
mc ls "$MINIO_ALIAS"