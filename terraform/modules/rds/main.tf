# RDS Module for E-Commerce Analytics Platform
# Provides managed PostgreSQL database with high availability and backup

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Random password for master user
resource "random_password" "master" {
  length  = 16
  special = true
}

# KMS Key for RDS encryption
resource "aws_kms_key" "rds" {
  description             = "RDS encryption key for ${var.db_name}"
  deletion_window_in_days = var.kms_key_deletion_window

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-rds-kms-key"
    Type = "KMSKey"
  })
}

resource "aws_kms_alias" "rds" {
  name          = "alias/${var.name_prefix}-rds-encryption"
  target_key_id = aws_kms_key.rds.key_id
}

# Parameter Group for PostgreSQL optimization
resource "aws_db_parameter_group" "main" {
  family = "postgres${var.engine_version}"
  name   = "${var.name_prefix}-pg${replace(var.engine_version, ".", "")}"

  # PostgreSQL configuration for analytics workloads
  parameter {
    name  = "shared_preload_libraries"
    value = "pg_stat_statements"
  }

  parameter {
    name  = "log_statement"
    value = "all"
  }

  parameter {
    name  = "log_duration"
    value = "1"
  }

  parameter {
    name  = "log_min_duration_statement"
    value = "1000"  # Log queries taking longer than 1 second
  }

  parameter {
    name  = "max_connections"
    value = var.max_connections
  }

  parameter {
    name  = "work_mem"
    value = var.work_mem_mb * 1024  # Convert MB to KB
  }

  parameter {
    name  = "maintenance_work_mem"
    value = var.maintenance_work_mem_mb * 1024  # Convert MB to KB
  }

  parameter {
    name  = "effective_cache_size"
    value = var.effective_cache_size_mb * 1024  # Convert MB to KB
  }

  parameter {
    name  = "random_page_cost"
    value = "1.1"  # Optimized for SSD storage
  }

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-parameter-group"
    Type = "ParameterGroup"
  })
}

# Option Group (if needed for specific extensions)
resource "aws_db_option_group" "main" {
  name                 = "${var.name_prefix}-option-group"
  option_group_description = "Option group for ${var.name_prefix} PostgreSQL"
  engine_name          = "postgres"
  major_engine_version = var.engine_version

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-option-group"
    Type = "OptionGroup"
  })
}

# Primary RDS Instance
resource "aws_db_instance" "main" {
  # Basic Configuration
  identifier     = var.db_instance_identifier
  engine         = "postgres"
  engine_version = var.engine_version
  instance_class = var.instance_class

  # Database Configuration
  allocated_storage       = var.allocated_storage
  max_allocated_storage   = var.max_allocated_storage
  storage_type           = var.storage_type
  storage_encrypted      = true
  kms_key_id            = aws_kms_key.rds.arn

  # Database Details
  db_name  = var.db_name
  username = var.master_username
  password = random_password.master.result
  port     = var.database_port

  # Network Configuration
  db_subnet_group_name   = var.db_subnet_group_name
  vpc_security_group_ids = [var.security_group_id]
  publicly_accessible    = false

  # Backup Configuration
  backup_retention_period = var.backup_retention_period
  backup_window          = var.backup_window
  maintenance_window     = var.maintenance_window

  # Monitoring and Logging
  monitoring_interval                   = var.monitoring_interval
  monitoring_role_arn                  = var.monitoring_interval > 0 ? aws_iam_role.rds_enhanced_monitoring[0].arn : null
  performance_insights_enabled         = var.performance_insights_enabled
  performance_insights_retention_period = var.performance_insights_enabled ? var.performance_insights_retention_period : null
  enabled_cloudwatch_logs_exports      = var.enabled_cloudwatch_logs_exports

  # High Availability
  multi_az = var.multi_az

  # Parameter and Option Groups
  parameter_group_name = aws_db_parameter_group.main.name
  option_group_name    = aws_db_option_group.main.name

  # Deletion Protection
  deletion_protection = var.deletion_protection
  skip_final_snapshot = var.skip_final_snapshot
  final_snapshot_identifier = var.skip_final_snapshot ? null : "${var.db_instance_identifier}-final-snapshot-${formatdate("YYYY-MM-DD-hhmm", timestamp())}"

  # Apply changes immediately (be careful in production)
  apply_immediately = var.apply_immediately

  # Auto minor version upgrade
  auto_minor_version_upgrade = var.auto_minor_version_upgrade

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-postgresql"
    Type = "RDSInstance"
  })

  lifecycle {
    ignore_changes = [
      password,
      final_snapshot_identifier
    ]
  }
}

# Read Replica (optional for scaling read workloads)
resource "aws_db_instance" "read_replica" {
  count = var.create_read_replica ? 1 : 0

  identifier = "${var.db_instance_identifier}-read-replica"

  # Replica Configuration
  replicate_source_db = aws_db_instance.main.identifier
  instance_class     = var.read_replica_instance_class

  # Network Configuration
  vpc_security_group_ids = [var.security_group_id]
  publicly_accessible    = false

  # Monitoring
  monitoring_interval = var.monitoring_interval
  monitoring_role_arn = var.monitoring_interval > 0 ? aws_iam_role.rds_enhanced_monitoring[0].arn : null
  performance_insights_enabled = var.performance_insights_enabled
  performance_insights_retention_period = var.performance_insights_enabled ? var.performance_insights_retention_period : null

  # Auto minor version upgrade
  auto_minor_version_upgrade = var.auto_minor_version_upgrade

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-postgresql-replica"
    Type = "RDSReadReplica"
  })
}

# Enhanced Monitoring IAM Role
resource "aws_iam_role" "rds_enhanced_monitoring" {
  count = var.monitoring_interval > 0 ? 1 : 0

  name = "${var.name_prefix}-rds-enhanced-monitoring"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "monitoring.rds.amazonaws.com"
        }
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "rds_enhanced_monitoring" {
  count = var.monitoring_interval > 0 ? 1 : 0

  role       = aws_iam_role.rds_enhanced_monitoring[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
}

# CloudWatch Alarms for monitoring
resource "aws_cloudwatch_metric_alarm" "database_cpu" {
  alarm_name          = "${var.name_prefix}-rds-cpu-utilization"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "CPUUtilization"
  namespace           = "AWS/RDS"
  period              = "120"
  statistic           = "Average"
  threshold           = "80"
  alarm_description   = "This metric monitors RDS CPU utilization"

  dimensions = {
    DBInstanceIdentifier = aws_db_instance.main.id
  }

  alarm_actions = var.alarm_sns_topic_arn != null ? [var.alarm_sns_topic_arn] : []

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-rds-cpu-alarm"
    Type = "CloudWatchAlarm"
  })
}

resource "aws_cloudwatch_metric_alarm" "database_disk_queue" {
  alarm_name          = "${var.name_prefix}-rds-disk-queue-depth"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "DiskQueueDepth"
  namespace           = "AWS/RDS"
  period              = "120"
  statistic           = "Average"
  threshold           = "64"
  alarm_description   = "This metric monitors RDS disk queue depth"

  dimensions = {
    DBInstanceIdentifier = aws_db_instance.main.id
  }

  alarm_actions = var.alarm_sns_topic_arn != null ? [var.alarm_sns_topic_arn] : []

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-rds-disk-queue-alarm"
    Type = "CloudWatchAlarm"
  })
}

resource "aws_cloudwatch_metric_alarm" "database_disk_free" {
  alarm_name          = "${var.name_prefix}-rds-free-storage-space"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "FreeStorageSpace"
  namespace           = "AWS/RDS"
  period              = "120"
  statistic           = "Average"
  threshold           = "2000000000"  # 2GB in bytes
  alarm_description   = "This metric monitors RDS free storage space"

  dimensions = {
    DBInstanceIdentifier = aws_db_instance.main.id
  }

  alarm_actions = var.alarm_sns_topic_arn != null ? [var.alarm_sns_topic_arn] : []

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-rds-storage-alarm"
    Type = "CloudWatchAlarm"
  })
}

resource "aws_cloudwatch_metric_alarm" "database_memory_freeable" {
  alarm_name          = "${var.name_prefix}-rds-freeable-memory"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "FreeableMemory"
  namespace           = "AWS/RDS"
  period              = "120"
  statistic           = "Average"
  threshold           = "128000000"  # 128MB in bytes
  alarm_description   = "This metric monitors RDS freeable memory"

  dimensions = {
    DBInstanceIdentifier = aws_db_instance.main.id
  }

  alarm_actions = var.alarm_sns_topic_arn != null ? [var.alarm_sns_topic_arn] : []

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-rds-memory-alarm"
    Type = "CloudWatchAlarm"
  })
}

# Store database password in AWS Secrets Manager
resource "aws_secretsmanager_secret" "db_password" {
  name                    = "${var.name_prefix}-db-password"
  description             = "Database password for ${var.name_prefix} PostgreSQL"
  recovery_window_in_days = var.secrets_recovery_window_days

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-db-password-secret"
    Type = "Secret"
  })
}

resource "aws_secretsmanager_secret_version" "db_password" {
  secret_id = aws_secretsmanager_secret.db_password.id
  secret_string = jsonencode({
    username = aws_db_instance.main.username
    password = random_password.master.result
    endpoint = aws_db_instance.main.endpoint
    port     = aws_db_instance.main.port
    dbname   = aws_db_instance.main.db_name
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}
