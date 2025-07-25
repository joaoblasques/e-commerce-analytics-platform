# S3 Module Variables

variable "bucket_name" {
  description = "Name of the S3 bucket"
  type        = string
}

variable "versioning_enabled" {
  description = "Whether to enable versioning on the S3 bucket"
  type        = bool
  default     = true
}

variable "kms_key_deletion_window" {
  description = "The waiting period, specified in number of days, after which KMS key is deleted"
  type        = number
  default     = 10
}

# Lifecycle Configuration
variable "raw_data_ia_transition_days" {
  description = "Days after which raw data transitions to IA storage"
  type        = number
  default     = 30
}

variable "raw_data_glacier_transition_days" {
  description = "Days after which raw data transitions to Glacier storage"
  type        = number
  default     = 90
}

variable "raw_data_deep_archive_transition_days" {
  description = "Days after which raw data transitions to Deep Archive storage"
  type        = number
  default     = 365
}

variable "raw_data_expiration_days" {
  description = "Days after which raw data expires (0 = never expire)"
  type        = number
  default     = 0
}

variable "processed_data_ia_transition_days" {
  description = "Days after which processed data transitions to IA storage"
  type        = number
  default     = 90
}

variable "processed_data_glacier_transition_days" {
  description = "Days after which processed data transitions to Glacier storage"
  type        = number
  default     = 365
}

variable "processed_data_expiration_days" {
  description = "Days after which processed data expires (0 = never expire)"
  type        = number
  default     = 0
}

variable "temp_data_expiration_days" {
  description = "Days after which temporary data expires"
  type        = number
  default     = 7
}

variable "logs_expiration_days" {
  description = "Days after which log data expires"
  type        = number
  default     = 365
}

variable "noncurrent_version_expiration_days" {
  description = "Days after which noncurrent versions expire"
  type        = number
  default     = 90
}

variable "incomplete_multipart_upload_days" {
  description = "Days after which incomplete multipart uploads are aborted"
  type        = number
  default     = 7
}

# Event Notifications
variable "enable_event_notifications" {
  description = "Whether to enable S3 event notifications"
  type        = bool
  default     = false
}

variable "lambda_notifications" {
  description = "List of Lambda function notifications"
  type = list(object({
    lambda_function_arn = string
    events              = list(string)
    filter_prefix       = string
    filter_suffix       = string
  }))
  default = []
}

variable "sqs_notifications" {
  description = "List of SQS queue notifications"
  type = list(object({
    queue_arn     = string
    events        = list(string)
    filter_prefix = string
    filter_suffix = string
  }))
  default = []
}

variable "sns_notifications" {
  description = "List of SNS topic notifications"
  type = list(object({
    topic_arn     = string
    events        = list(string)
    filter_prefix = string
    filter_suffix = string
  }))
  default = []
}

# Access Logging
variable "enable_access_logging" {
  description = "Whether to enable S3 access logging"
  type        = bool
  default     = false
}

variable "access_log_bucket_name" {
  description = "Name of the bucket to store access logs (if null, creates a new bucket)"
  type        = string
  default     = null
}

variable "access_log_prefix" {
  description = "Prefix for access log objects"
  type        = string
  default     = "access-logs/"
}

# CORS Configuration
variable "enable_cors" {
  description = "Whether to enable CORS configuration"
  type        = bool
  default     = false
}

variable "cors_allowed_headers" {
  description = "List of allowed headers for CORS"
  type        = list(string)
  default     = ["*"]
}

variable "cors_allowed_methods" {
  description = "List of allowed methods for CORS"
  type        = list(string)
  default     = ["GET", "PUT", "POST"]
}

variable "cors_allowed_origins" {
  description = "List of allowed origins for CORS"
  type        = list(string)
  default     = ["*"]
}

variable "cors_expose_headers" {
  description = "List of headers to expose for CORS"
  type        = list(string)
  default     = ["ETag"]
}

variable "cors_max_age_seconds" {
  description = "Max age in seconds for CORS"
  type        = number
  default     = 3000
}

# Bucket Policy
variable "additional_bucket_policy_statements" {
  description = "Additional bucket policy statements"
  type        = list(any)
  default     = []
}

# CloudWatch Alarms
variable "bucket_size_alarm_threshold" {
  description = "Bucket size threshold in bytes for CloudWatch alarm"
  type        = number
  default     = 1073741824000  # 1TB
}

variable "alarm_sns_topic_arn" {
  description = "SNS topic ARN for CloudWatch alarms"
  type        = string
  default     = null
}

# S3 Features
variable "enable_inventory" {
  description = "Whether to enable S3 inventory"
  type        = bool
  default     = true
}

variable "enable_analytics" {
  description = "Whether to enable S3 analytics"
  type        = bool
  default     = true
}

variable "enable_intelligent_tiering" {
  description = "Whether to enable S3 intelligent tiering"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}
