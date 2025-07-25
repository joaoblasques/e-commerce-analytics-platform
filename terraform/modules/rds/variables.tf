# RDS Module Variables

variable "name_prefix" {
  description = "Prefix for all resource names"
  type        = string
}

variable "db_instance_identifier" {
  description = "The DB instance identifier"
  type        = string
}

variable "db_name" {
  description = "The name of the database to create when the DB instance is created"
  type        = string
  default     = "ecommerce_analytics"
}

variable "master_username" {
  description = "Username for the master DB user"
  type        = string
  default     = "ecap_admin"
}

# Engine Configuration
variable "engine_version" {
  description = "The engine version to use"
  type        = string
  default     = "15.4"
}

variable "instance_class" {
  description = "The instance type of the RDS instance"
  type        = string
  default     = "db.t3.medium"
}

# Storage Configuration
variable "allocated_storage" {
  description = "The allocated storage in gigabytes"
  type        = number
  default     = 100
}

variable "max_allocated_storage" {
  description = "The upper limit to which Amazon RDS can automatically scale the storage"
  type        = number
  default     = 1000
}

variable "storage_type" {
  description = "One of 'standard' (magnetic), 'gp2' (general purpose SSD), or 'io1' (provisioned IOPS SSD)"
  type        = string
  default     = "gp2"
}

# Network Configuration
variable "database_port" {
  description = "The port on which the DB accepts connections"
  type        = number
  default     = 5432
}

variable "db_subnet_group_name" {
  description = "Name of DB subnet group"
  type        = string
}

variable "security_group_id" {
  description = "The security group ID to associate with the RDS instance"
  type        = string
}

# Backup Configuration
variable "backup_retention_period" {
  description = "The days to retain backups for"
  type        = number
  default     = 7
}

variable "backup_window" {
  description = "The daily time range (in UTC) during which automated backups are created"
  type        = string
  default     = "03:00-04:00"
}

variable "maintenance_window" {
  description = "The window to perform maintenance in"
  type        = string
  default     = "sun:04:00-sun:05:00"
}

# High Availability
variable "multi_az" {
  description = "Specifies if the RDS instance is multi-AZ"
  type        = bool
  default     = true
}

# Monitoring Configuration
variable "monitoring_interval" {
  description = "The interval, in seconds, between points when Enhanced Monitoring metrics are collected"
  type        = number
  default     = 60

  validation {
    condition = contains([
      0, 1, 5, 10, 15, 30, 60
    ], var.monitoring_interval)
    error_message = "Monitoring interval must be one of: 0, 1, 5, 10, 15, 30, 60."
  }
}

variable "performance_insights_enabled" {
  description = "Specifies whether Performance Insights are enabled"
  type        = bool
  default     = true
}

variable "performance_insights_retention_period" {
  description = "The amount of time in days to retain Performance Insights data"
  type        = number
  default     = 7

  validation {
    condition = contains([
      7, 31, 62, 93, 124, 155, 186, 217, 248, 279, 310, 341, 372, 403, 434, 465, 496, 527, 558, 589, 620, 651, 682, 713, 731
    ], var.performance_insights_retention_period)
    error_message = "Performance Insights retention period must be 7 days or a multiple of 31 days up to 731 days."
  }
}

variable "enabled_cloudwatch_logs_exports" {
  description = "List of log types to export to CloudWatch"
  type        = list(string)
  default     = ["postgresql"]
}

# PostgreSQL Specific Parameters
variable "max_connections" {
  description = "Maximum number of connections"
  type        = string
  default     = "200"
}

variable "work_mem_mb" {
  description = "Amount of memory to be used by internal sort operations and hash tables (MB)"
  type        = number
  default     = 4
}

variable "maintenance_work_mem_mb" {
  description = "Maximum amount of memory to be used by maintenance operations (MB)"
  type        = number
  default     = 64
}

variable "effective_cache_size_mb" {
  description = "Effective cache size (MB)"
  type        = number
  default     = 1024
}

# Read Replica Configuration
variable "create_read_replica" {
  description = "Whether to create a read replica"
  type        = bool
  default     = false
}

variable "read_replica_instance_class" {
  description = "The instance type of the read replica"
  type        = string
  default     = "db.t3.medium"
}

# Security Configuration
variable "deletion_protection" {
  description = "If the DB instance should have deletion protection enabled"
  type        = bool
  default     = true
}

variable "skip_final_snapshot" {
  description = "Determines whether a final DB snapshot is created before the DB instance is deleted"
  type        = bool
  default     = false
}

variable "apply_immediately" {
  description = "Specifies whether any database modifications are applied immediately"
  type        = bool
  default     = false
}

variable "auto_minor_version_upgrade" {
  description = "Indicates that minor engine upgrades will be applied automatically"
  type        = bool
  default     = false
}

variable "kms_key_deletion_window" {
  description = "The waiting period, specified in number of days, after which KMS key is deleted"
  type        = number
  default     = 10
}

variable "secrets_recovery_window_days" {
  description = "Number of days that AWS Secrets Manager waits before deleting the secret"
  type        = number
  default     = 30
}

# CloudWatch Alarms
variable "alarm_sns_topic_arn" {
  description = "SNS topic ARN for CloudWatch alarms"
  type        = string
  default     = null
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}
