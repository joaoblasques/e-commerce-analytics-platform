# S3 Module Outputs

output "bucket_id" {
  description = "The name of the bucket"
  value       = aws_s3_bucket.main.id
}

output "bucket_arn" {
  description = "The ARN of the bucket"
  value       = aws_s3_bucket.main.arn
}

output "bucket_domain_name" {
  description = "The bucket domain name"
  value       = aws_s3_bucket.main.bucket_domain_name
}

output "bucket_regional_domain_name" {
  description = "The bucket regional domain name"
  value       = aws_s3_bucket.main.bucket_regional_domain_name
}

output "bucket_hosted_zone_id" {
  description = "The Route 53 Hosted Zone ID for this bucket's region"
  value       = aws_s3_bucket.main.hosted_zone_id
}

output "bucket_region" {
  description = "The AWS region this bucket resides in"
  value       = aws_s3_bucket.main.region
}

# KMS Key outputs
output "kms_key_id" {
  description = "The globally unique identifier for the encryption key"
  value       = aws_kms_key.s3.key_id
}

output "kms_key_arn" {
  description = "The Amazon Resource Name (ARN) of the encryption key"
  value       = aws_kms_key.s3.arn
}

output "kms_alias_arn" {
  description = "The Amazon Resource Name (ARN) of the key alias"
  value       = aws_kms_alias.s3.arn
}

# Access logs bucket outputs
output "access_logs_bucket_id" {
  description = "The name of the access logs bucket"
  value       = var.enable_access_logging && var.access_log_bucket_name == null ? aws_s3_bucket.access_logs[0].id : var.access_log_bucket_name
}

output "access_logs_bucket_arn" {
  description = "The ARN of the access logs bucket"
  value       = var.enable_access_logging && var.access_log_bucket_name == null ? aws_s3_bucket.access_logs[0].arn : null
}

# CloudWatch alarm outputs
output "bucket_size_alarm_id" {
  description = "The ID of the bucket size alarm"
  value       = aws_cloudwatch_metric_alarm.bucket_size.id
}

output "bucket_size_alarm_arn" {
  description = "The ARN of the bucket size alarm"
  value       = aws_cloudwatch_metric_alarm.bucket_size.arn
}

# Data Lake Structure outputs
output "data_lake_structure" {
  description = "Recommended data lake folder structure"
  value = {
    raw_data = {
      prefix      = "raw/"
      description = "Raw incoming data from various sources"
      partitioning = "year/month/day/hour"
      example     = "raw/transactions/2024/01/15/12/"
    }
    processed_data = {
      prefix      = "processed/"
      description = "Cleaned and transformed data"
      partitioning = "year/month/day"
      example     = "processed/customer_analytics/2024/01/15/"
    }
    curated_data = {
      prefix      = "curated/"
      description = "Business-ready aggregated data"
      partitioning = "year/month"
      example     = "curated/monthly_reports/2024/01/"
    }
    temp_data = {
      prefix      = "temp/"
      description = "Temporary processing data"
      lifecycle   = "${var.temp_data_expiration_days} days retention"
      example     = "temp/spark_checkpoints/job_123/"
    }
    logs = {
      prefix      = "logs/"
      description = "Application and processing logs"
      lifecycle   = "${var.logs_expiration_days} days retention"
      example     = "logs/spark/2024/01/15/"
    }
    models = {
      prefix      = "models/"
      description = "Machine learning models and artifacts"
      partitioning = "model_type/version"
      example     = "models/fraud_detection/v1.2.0/"
    }
    metadata = {
      prefix      = "metadata/"
      description = "Data catalog and schema information"
      example     = "metadata/schemas/transactions.json"
    }
  }
}

# Environment variables for applications
output "environment_variables" {
  description = "Environment variables for applications using this S3 bucket"
  value = {
    S3_BUCKET_NAME        = aws_s3_bucket.main.id
    S3_BUCKET_ARN         = aws_s3_bucket.main.arn
    S3_BUCKET_REGION      = aws_s3_bucket.main.region
    S3_KMS_KEY_ID         = aws_kms_key.s3.key_id
    S3_KMS_KEY_ARN        = aws_kms_key.s3.arn
    S3_RAW_PREFIX         = "raw/"
    S3_PROCESSED_PREFIX   = "processed/"
    S3_CURATED_PREFIX     = "curated/"
    S3_TEMP_PREFIX        = "temp/"
    S3_LOGS_PREFIX        = "logs/"
    S3_MODELS_PREFIX      = "models/"
    S3_METADATA_PREFIX    = "metadata/"
  }
}

# Spark Configuration
output "spark_configuration" {
  description = "Spark configuration for S3 access"
  value = {
    "spark.hadoop.fs.s3a.impl"                          = "org.apache.hadoop.fs.s3a.S3AFileSystem"
    "spark.hadoop.fs.s3a.aws.credentials.provider"      = "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider"
    "spark.hadoop.fs.s3a.server-side-encryption-algorithm" = "SSE-KMS"
    "spark.hadoop.fs.s3a.server-side-encryption.key"    = aws_kms_key.s3.arn
    "spark.hadoop.fs.s3a.bucket.${aws_s3_bucket.main.id}.server-side-encryption-algorithm" = "SSE-KMS"
    "spark.hadoop.fs.s3a.bucket.${aws_s3_bucket.main.id}.server-side-encryption.key" = aws_kms_key.s3.arn
    "spark.hadoop.fs.s3a.multipart.size"                = "104857600"  # 100MB
    "spark.hadoop.fs.s3a.fast.upload"                   = "true"
    "spark.hadoop.fs.s3a.block.size"                    = "67108864"   # 64MB
    "spark.sql.adaptive.enabled"                        = "true"
    "spark.sql.adaptive.coalescePartitions.enabled"     = "true"
  }
}

# Delta Lake Configuration
output "delta_lake_configuration" {
  description = "Delta Lake configuration for S3"
  value = {
    delta_storage_path     = "s3a://${aws_s3_bucket.main.id}/delta/"
    delta_log_store_class  = "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
    warehouse_dir          = "s3a://${aws_s3_bucket.main.id}/delta/warehouse/"
    checkpoints_location   = "s3a://${aws_s3_bucket.main.id}/temp/checkpoints/"
  }
}

# Cost optimization information
output "cost_optimization_features" {
  description = "Enabled cost optimization features"
  value = {
    lifecycle_policies = {
      raw_data_ia_days               = var.raw_data_ia_transition_days
      raw_data_glacier_days          = var.raw_data_glacier_transition_days
      raw_data_deep_archive_days     = var.raw_data_deep_archive_transition_days
      processed_data_ia_days         = var.processed_data_ia_transition_days
      processed_data_glacier_days    = var.processed_data_glacier_transition_days
      temp_data_expiration_days      = var.temp_data_expiration_days
      logs_expiration_days           = var.logs_expiration_days
    }
    intelligent_tiering_enabled = var.enable_intelligent_tiering
    inventory_enabled          = var.enable_inventory
    analytics_enabled          = var.enable_analytics
    versioning_enabled         = var.versioning_enabled
    encryption_enabled         = true
  }
}

# Data lake access patterns
output "access_patterns" {
  description = "Recommended access patterns for the data lake"
  value = {
    real_time_ingestion = {
      path_pattern = "raw/{source}/{year}/{month}/{day}/{hour}/"
      use_case     = "Streaming data ingestion from Kafka"
      access_frequency = "High (continuous)"
    }
    batch_processing = {
      path_pattern = "processed/{dataset}/{year}/{month}/{day}/"
      use_case     = "Daily/hourly batch processing jobs"
      access_frequency = "Medium (daily/hourly)"
    }
    analytics_queries = {
      path_pattern = "curated/{domain}/{year}/{month}/"
      use_case     = "Business intelligence and reporting"
      access_frequency = "Low (weekly/monthly)"
    }
    model_artifacts = {
      path_pattern = "models/{model_type}/{version}/"
      use_case     = "ML model storage and versioning"
      access_frequency = "Low (on-demand)"
    }
    temporary_processing = {
      path_pattern = "temp/{job_id}/"
      use_case     = "Temporary Spark processing data"
      access_frequency = "High (short duration)"
    }
  }
}
