# S3 Module for E-Commerce Analytics Platform
# Provides data lake storage with lifecycle management and cost optimization

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# KMS Key for S3 encryption
resource "aws_kms_key" "s3" {
  description             = "S3 encryption key for ${var.bucket_name}"
  deletion_window_in_days = var.kms_key_deletion_window

  tags = merge(var.tags, {
    Name = "${var.bucket_name}-s3-kms-key"
    Type = "KMSKey"
  })
}

resource "aws_kms_alias" "s3" {
  name          = "alias/${var.bucket_name}-s3-encryption"
  target_key_id = aws_kms_key.s3.key_id
}

# Main S3 Bucket for Data Lake
resource "aws_s3_bucket" "main" {
  bucket = var.bucket_name

  tags = merge(var.tags, {
    Name = var.bucket_name
    Type = "S3Bucket"
    Purpose = "DataLake"
  })
}

# S3 Bucket Versioning
resource "aws_s3_bucket_versioning" "main" {
  bucket = aws_s3_bucket.main.id
  versioning_configuration {
    status = var.versioning_enabled ? "Enabled" : "Disabled"
  }
}

# S3 Bucket Server-side encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "main" {
  bucket = aws_s3_bucket.main.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.s3.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

# S3 Bucket Public Access Block
resource "aws_s3_bucket_public_access_block" "main" {
  bucket = aws_s3_bucket.main.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 Bucket Lifecycle Configuration
resource "aws_s3_bucket_lifecycle_configuration" "main" {
  bucket = aws_s3_bucket.main.id

  # Raw data lifecycle
  rule {
    id     = "raw-data-lifecycle"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    transition {
      days          = var.raw_data_ia_transition_days
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = var.raw_data_glacier_transition_days
      storage_class = "GLACIER"
    }

    transition {
      days          = var.raw_data_deep_archive_transition_days
      storage_class = "DEEP_ARCHIVE"
    }

    dynamic "expiration" {
      for_each = var.raw_data_expiration_days > 0 ? [1] : []
      content {
        days = var.raw_data_expiration_days
      }
    }

    noncurrent_version_transition {
      noncurrent_days = 30
      storage_class   = "STANDARD_IA"
    }

    noncurrent_version_transition {
      noncurrent_days = 60
      storage_class   = "GLACIER"
    }

    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_expiration_days
    }
  }

  # Processed data lifecycle
  rule {
    id     = "processed-data-lifecycle"
    status = "Enabled"

    filter {
      prefix = "processed/"
    }

    transition {
      days          = var.processed_data_ia_transition_days
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = var.processed_data_glacier_transition_days
      storage_class = "GLACIER"
    }

    dynamic "expiration" {
      for_each = var.processed_data_expiration_days > 0 ? [1] : []
      content {
        days = var.processed_data_expiration_days
      }
    }
  }

  # Temporary/staging data lifecycle
  rule {
    id     = "temp-data-lifecycle"
    status = "Enabled"

    filter {
      prefix = "temp/"
    }

    expiration {
      days = var.temp_data_expiration_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = var.incomplete_multipart_upload_days
    }
  }

  # Logs lifecycle
  rule {
    id     = "logs-lifecycle"
    status = "Enabled"

    filter {
      prefix = "logs/"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    expiration {
      days = var.logs_expiration_days
    }
  }

  depends_on = [aws_s3_bucket_versioning.main]
}

# S3 Bucket Notification for Lambda triggers
resource "aws_s3_bucket_notification" "main" {
  count  = var.enable_event_notifications ? 1 : 0
  bucket = aws_s3_bucket.main.id

  # Lambda function triggers for data processing
  dynamic "lambda_function" {
    for_each = var.lambda_notifications
    content {
      lambda_function_arn = lambda_function.value.lambda_function_arn
      events              = lambda_function.value.events
      filter_prefix       = lambda_function.value.filter_prefix
      filter_suffix       = lambda_function.value.filter_suffix
    }
  }

  # SQS queue notifications
  dynamic "queue" {
    for_each = var.sqs_notifications
    content {
      queue_arn     = queue.value.queue_arn
      events        = queue.value.events
      filter_prefix = queue.value.filter_prefix
      filter_suffix = queue.value.filter_suffix
    }
  }

  # SNS topic notifications
  dynamic "topic" {
    for_each = var.sns_notifications
    content {
      topic_arn     = topic.value.topic_arn
      events        = topic.value.events
      filter_prefix = topic.value.filter_prefix
      filter_suffix = topic.value.filter_suffix
    }
  }
}

# S3 Bucket Logging
resource "aws_s3_bucket_logging" "main" {
  count  = var.enable_access_logging ? 1 : 0
  bucket = aws_s3_bucket.main.id

  target_bucket = var.access_log_bucket_name != null ? var.access_log_bucket_name : aws_s3_bucket.access_logs[0].id
  target_prefix = var.access_log_prefix
}

# Access logs bucket (if not provided)
resource "aws_s3_bucket" "access_logs" {
  count  = var.enable_access_logging && var.access_log_bucket_name == null ? 1 : 0
  bucket = "${var.bucket_name}-access-logs"

  tags = merge(var.tags, {
    Name = "${var.bucket_name}-access-logs"
    Type = "S3Bucket"
    Purpose = "AccessLogs"
  })
}

resource "aws_s3_bucket_public_access_block" "access_logs" {
  count  = var.enable_access_logging && var.access_log_bucket_name == null ? 1 : 0
  bucket = aws_s3_bucket.access_logs[0].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 Bucket CORS configuration
resource "aws_s3_bucket_cors_configuration" "main" {
  count  = var.enable_cors ? 1 : 0
  bucket = aws_s3_bucket.main.id

  cors_rule {
    allowed_headers = var.cors_allowed_headers
    allowed_methods = var.cors_allowed_methods
    allowed_origins = var.cors_allowed_origins
    expose_headers  = var.cors_expose_headers
    max_age_seconds = var.cors_max_age_seconds
  }
}

# S3 Bucket Policy for cross-account access and service access
resource "aws_s3_bucket_policy" "main" {
  bucket = aws_s3_bucket.main.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      # Deny insecure connections
      {
        Sid       = "DenyInsecureConnections"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          aws_s3_bucket.main.arn,
          "${aws_s3_bucket.main.arn}/*"
        ]
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      },
      # Deny unencrypted uploads
      {
        Sid       = "DenyUnencryptedUploads"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:PutObject"
        Resource  = "${aws_s3_bucket.main.arn}/*"
        Condition = {
          StringNotEquals = {
            "s3:x-amz-server-side-encryption" = "aws:kms"
          }
        }
      }
    ], var.additional_bucket_policy_statements)
  })
}

# CloudWatch metrics for S3
resource "aws_cloudwatch_metric_alarm" "bucket_size" {
  alarm_name          = "${var.bucket_name}-bucket-size"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "BucketSizeBytes"
  namespace           = "AWS/S3"
  period              = "86400"  # 24 hours
  statistic           = "Average"
  threshold           = var.bucket_size_alarm_threshold
  alarm_description   = "This metric monitors S3 bucket size"
  treat_missing_data  = "notBreaching"

  dimensions = {
    BucketName  = aws_s3_bucket.main.id
    StorageType = "StandardStorage"
  }

  alarm_actions = var.alarm_sns_topic_arn != null ? [var.alarm_sns_topic_arn] : []

  tags = merge(var.tags, {
    Name = "${var.bucket_name}-size-alarm"
    Type = "CloudWatchAlarm"
  })
}

# S3 Inventory configuration for cost optimization
resource "aws_s3_bucket_inventory" "main" {
  count  = var.enable_inventory ? 1 : 0
  bucket = aws_s3_bucket.main.id
  name   = "main-inventory"

  included_object_versions = "All"

  schedule {
    frequency = "Daily"
  }

  destination {
    bucket {
      format     = "CSV"
      bucket_arn = aws_s3_bucket.main.arn
      prefix     = "inventory/"

      encryption {
        sse_kms {
          key_id = aws_kms_key.s3.arn
        }
      }
    }
  }

  optional_fields = [
    "Size",
    "LastModifiedDate",
    "StorageClass",
    "ETag",
    "IsMultipartUploaded",
    "ReplicationStatus",
    "EncryptionStatus",
    "ObjectLockRetainUntilDate",
    "ObjectLockMode",
    "ObjectLockLegalHoldStatus",
    "IntelligentTieringAccessTier"
  ]
}

# S3 Analytics configuration for storage class analysis
resource "aws_s3_bucket_analytics_configuration" "main" {
  count  = var.enable_analytics ? 1 : 0
  bucket = aws_s3_bucket.main.id
  name   = "main-analytics"

  filter {
    prefix = "raw/"
  }

  storage_class_analysis {
    data_export {
      destination {
        s3_bucket_destination {
          bucket_arn = aws_s3_bucket.main.arn
          prefix     = "analytics/"
          format     = "CSV"
        }
      }
      output_schema_version = "V_1"
    }
  }
}

# S3 Intelligent Tiering configuration
resource "aws_s3_bucket_intelligent_tiering_configuration" "main" {
  count  = var.enable_intelligent_tiering ? 1 : 0
  bucket = aws_s3_bucket.main.id
  name   = "main-intelligent-tiering"

  filter {
    prefix = "processed/"
  }

  tiering {
    access_tier = "DEEP_ARCHIVE_ACCESS"
    days        = 180
  }

  tiering {
    access_tier = "ARCHIVE_ACCESS"
    days        = 90
  }
}
