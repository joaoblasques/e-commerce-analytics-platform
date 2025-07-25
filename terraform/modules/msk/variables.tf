# MSK Module Variables

variable "cluster_name" {
  description = "Name of the MSK cluster"
  type        = string
}

variable "kafka_version" {
  description = "Version of Apache Kafka"
  type        = string
  default     = "2.8.1"
}

variable "number_of_broker_nodes" {
  description = "Number of broker nodes in the cluster"
  type        = number
  default     = 3

  validation {
    condition     = var.number_of_broker_nodes >= 2
    error_message = "Number of broker nodes must be at least 2."
  }
}

variable "broker_instance_type" {
  description = "Instance type for Kafka brokers"
  type        = string
  default     = "kafka.t3.small"
}

variable "broker_ebs_volume_size" {
  description = "Size of the EBS volume for each broker (GB)"
  type        = number
  default     = 100
}

variable "subnet_ids" {
  description = "List of subnet IDs where brokers will be deployed"
  type        = list(string)
}

variable "security_group_id" {
  description = "Security group ID for the MSK cluster"
  type        = string
}

# Kafka Configuration
variable "auto_create_topics_enable" {
  description = "Whether to enable auto creation of topics"
  type        = bool
  default     = true
}

variable "default_replication_factor" {
  description = "Default replication factor for topics"
  type        = number
  default     = 3
}

variable "min_insync_replicas" {
  description = "Minimum number of in-sync replicas"
  type        = number
  default     = 2
}

variable "num_io_threads" {
  description = "Number of I/O threads"
  type        = number
  default     = 8
}

variable "num_network_threads" {
  description = "Number of network threads"
  type        = number
  default     = 5
}

variable "num_partitions" {
  description = "Default number of partitions for auto-created topics"
  type        = number
  default     = 3
}

variable "num_replica_fetchers" {
  description = "Number of replica fetcher threads"
  type        = number
  default     = 2
}

variable "replica_lag_time_max_ms" {
  description = "Maximum time a replica can lag behind the leader"
  type        = number
  default     = 30000
}

variable "socket_receive_buffer_bytes" {
  description = "Socket receive buffer size"
  type        = number
  default     = 102400
}

variable "socket_request_max_bytes" {
  description = "Maximum socket request size"
  type        = number
  default     = 104857600
}

variable "socket_send_buffer_bytes" {
  description = "Socket send buffer size"
  type        = number
  default     = 102400
}

variable "unclean_leader_election_enable" {
  description = "Whether to enable unclean leader election"
  type        = bool
  default     = false
}

variable "log_retention_hours" {
  description = "Log retention time in hours"
  type        = number
  default     = 168  # 7 days
}

variable "log_retention_bytes" {
  description = "Log retention size in bytes (-1 for unlimited)"
  type        = number
  default     = -1
}

variable "log_segment_bytes" {
  description = "Log segment size in bytes"
  type        = number
  default     = 1073741824  # 1GB
}

variable "log_cleanup_policy" {
  description = "Log cleanup policy"
  type        = string
  default     = "delete"
}

variable "compression_type" {
  description = "Compression type for topics"
  type        = string
  default     = "producer"
}

# Encryption Configuration
variable "client_broker_encryption" {
  description = "Encryption setting for data in transit between clients and brokers"
  type        = string
  default     = "TLS"

  validation {
    condition = contains([
      "PLAINTEXT", "TLS", "TLS_PLAINTEXT"
    ], var.client_broker_encryption)
    error_message = "Client broker encryption must be one of: PLAINTEXT, TLS, TLS_PLAINTEXT."
  }
}

variable "in_cluster_encryption" {
  description = "Whether to enable in-cluster encryption"
  type        = bool
  default     = true
}

variable "kms_key_deletion_window" {
  description = "The waiting period, specified in number of days, after which KMS key is deleted"
  type        = number
  default     = 10
}

# Monitoring Configuration
variable "enhanced_monitoring" {
  description = "Level of enhanced monitoring"
  type        = string
  default     = "PER_BROKER"

  validation {
    condition = contains([
      "DEFAULT", "PER_BROKER", "PER_TOPIC_PER_BROKER", "PER_TOPIC_PER_PARTITION"
    ], var.enhanced_monitoring)
    error_message = "Enhanced monitoring must be one of: DEFAULT, PER_BROKER, PER_TOPIC_PER_BROKER, PER_TOPIC_PER_PARTITION."
  }
}

variable "enable_prometheus_monitoring" {
  description = "Whether to enable Prometheus monitoring"
  type        = bool
  default     = true
}

variable "prometheus_jmx_exporter_enabled" {
  description = "Whether to enable JMX exporter for Prometheus"
  type        = bool
  default     = true
}

variable "prometheus_node_exporter_enabled" {
  description = "Whether to enable Node exporter for Prometheus"
  type        = bool
  default     = true
}

# Logging Configuration
variable "enable_cloudwatch_logs" {
  description = "Whether to enable CloudWatch logs"
  type        = bool
  default     = true
}

variable "enable_firehose_logs" {
  description = "Whether to enable Kinesis Data Firehose logs"
  type        = bool
  default     = false
}

variable "enable_s3_logs" {
  description = "Whether to enable S3 logs"
  type        = bool
  default     = false
}

variable "cloudwatch_log_retention_days" {
  description = "Number of days to retain CloudWatch logs"
  type        = number
  default     = 30
}

variable "firehose_delivery_stream_name" {
  description = "Name of the Kinesis Data Firehose delivery stream"
  type        = string
  default     = null
}

variable "s3_logs_bucket_name" {
  description = "Name of the S3 bucket for logs"
  type        = string
  default     = null
}

variable "s3_logs_prefix" {
  description = "Prefix for S3 logs"
  type        = string
  default     = "kafka-logs/"
}

# Authentication Configuration
variable "enable_client_authentication" {
  description = "Whether to enable client authentication"
  type        = bool
  default     = false
}

variable "enable_sasl_authentication" {
  description = "Whether to enable SASL authentication"
  type        = bool
  default     = false
}

variable "enable_sasl_scram" {
  description = "Whether to enable SASL SCRAM authentication"
  type        = bool
  default     = false
}

variable "enable_sasl_iam" {
  description = "Whether to enable SASL IAM authentication"
  type        = bool
  default     = false
}

variable "enable_tls_authentication" {
  description = "Whether to enable TLS authentication"
  type        = bool
  default     = false
}

variable "certificate_authority_arns" {
  description = "List of certificate authority ARNs for TLS authentication"
  type        = list(string)
  default     = []
}

variable "enable_unauthenticated_access" {
  description = "Whether to enable unauthenticated access"
  type        = bool
  default     = true
}

# Topic Configuration
variable "transactions_topic_partitions" {
  description = "Number of partitions for transactions topic"
  type        = number
  default     = 6
}

variable "user_events_topic_partitions" {
  description = "Number of partitions for user events topic"
  type        = number
  default     = 6
}

variable "product_updates_topic_partitions" {
  description = "Number of partitions for product updates topic"
  type        = number
  default     = 3
}

variable "fraud_alerts_topic_partitions" {
  description = "Number of partitions for fraud alerts topic"
  type        = number
  default     = 3
}

variable "create_topics_config" {
  description = "Whether to create topics configuration file"
  type        = bool
  default     = true
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
