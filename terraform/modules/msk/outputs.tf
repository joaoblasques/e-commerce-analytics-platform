# MSK Module Outputs

output "cluster_arn" {
  description = "Amazon Resource Name (ARN) of the MSK cluster"
  value       = aws_msk_cluster.main.arn
}

output "cluster_name" {
  description = "Name of the MSK cluster"
  value       = aws_msk_cluster.main.cluster_name
}

output "bootstrap_brokers" {
  description = "Plaintext connection host:port pairs"
  value       = aws_msk_cluster.main.bootstrap_brokers
}

output "bootstrap_brokers_tls" {
  description = "TLS connection host:port pairs"
  value       = aws_msk_cluster.main.bootstrap_brokers_tls
}

output "bootstrap_brokers_sasl_scram" {
  description = "SASL/SCRAM connection host:port pairs"
  value       = aws_msk_cluster.main.bootstrap_brokers_sasl_scram
}

output "bootstrap_brokers_sasl_iam" {
  description = "SASL/IAM connection host:port pairs"
  value       = aws_msk_cluster.main.bootstrap_brokers_sasl_iam
}

output "zookeeper_connect_string" {
  description = "A comma separated list of one or more hostname:port pairs to use to connect to the Apache Zookeeper cluster"
  value       = aws_msk_cluster.main.zookeeper_connect_string
}

output "current_version" {
  description = "Current version of the MSK Cluster used for updates"
  value       = aws_msk_cluster.main.current_version
}

# Configuration outputs
output "configuration_arn" {
  description = "Amazon Resource Name (ARN) of the configuration"
  value       = aws_msk_configuration.main.arn
}

output "configuration_latest_revision" {
  description = "Latest revision of the configuration"
  value       = aws_msk_configuration.main.latest_revision
}

# KMS Key outputs
output "kms_key_id" {
  description = "The globally unique identifier for the encryption key"
  value       = aws_kms_key.msk.key_id
}

output "kms_key_arn" {
  description = "The Amazon Resource Name (ARN) of the encryption key"
  value       = aws_kms_key.msk.arn
}

# CloudWatch Log Group
output "cloudwatch_log_group_name" {
  description = "Name of the CloudWatch log group"
  value       = aws_cloudwatch_log_group.msk.name
}

output "cloudwatch_log_group_arn" {
  description = "ARN of the CloudWatch log group"
  value       = aws_cloudwatch_log_group.msk.arn
}

# CloudWatch Alarms
output "cpu_alarm_id" {
  description = "The ID of the CPU utilization alarm"
  value       = aws_cloudwatch_metric_alarm.kafka_cpu_utilization.id
}

output "disk_alarm_id" {
  description = "The ID of the disk usage alarm"
  value       = aws_cloudwatch_metric_alarm.kafka_disk_usage.id
}

output "memory_alarm_id" {
  description = "The ID of the memory usage alarm"
  value       = aws_cloudwatch_metric_alarm.kafka_memory_usage.id
}

# Topic configuration for applications
output "topics_configuration" {
  description = "Kafka topics configuration"
  value = {
    transactions = {
      name       = "transactions"
      partitions = var.transactions_topic_partitions
    }
    user_events = {
      name       = "user-events"
      partitions = var.user_events_topic_partitions
    }
    product_updates = {
      name       = "product-updates"
      partitions = var.product_updates_topic_partitions
    }
    fraud_alerts = {
      name       = "fraud-alerts"
      partitions = var.fraud_alerts_topic_partitions
    }
  }
}

# Connection information for applications
output "connection_info" {
  description = "Connection information for Kafka clients"
  value = {
    cluster_name                  = aws_msk_cluster.main.cluster_name
    bootstrap_brokers            = aws_msk_cluster.main.bootstrap_brokers
    bootstrap_brokers_tls        = aws_msk_cluster.main.bootstrap_brokers_tls
    bootstrap_brokers_sasl_scram = aws_msk_cluster.main.bootstrap_brokers_sasl_scram
    bootstrap_brokers_sasl_iam   = aws_msk_cluster.main.bootstrap_brokers_sasl_iam
    zookeeper_connect_string     = aws_msk_cluster.main.zookeeper_connect_string
    security_protocol            = var.client_broker_encryption == "TLS" ? "SSL" : "PLAINTEXT"
  }
}

# Kafka client configuration template
output "kafka_client_properties" {
  description = "Kafka client configuration properties"
  value = templatefile("${path.module}/templates/client.properties.tpl", {
    bootstrap_servers    = aws_msk_cluster.main.bootstrap_brokers_tls
    security_protocol    = var.client_broker_encryption == "TLS" ? "SSL" : "PLAINTEXT"
    ssl_truststore_location = "/opt/kafka/config/kafka.client.truststore.jks"
    ssl_keystore_location = "/opt/kafka/config/kafka.client.keystore.jks"
  })
}

# Schema Registry configuration (if using Confluent Schema Registry)
output "schema_registry_config" {
  description = "Configuration for Schema Registry integration"
  value = {
    bootstrap_servers = aws_msk_cluster.main.bootstrap_brokers_tls
    cluster_name     = aws_msk_cluster.main.cluster_name
    topics = [
      "_schemas",
      "__consumer_offsets",
      "__transaction_state"
    ]
  }
}

# Environment variables for containerized applications
output "environment_variables" {
  description = "Environment variables for containerized applications"
  value = {
    KAFKA_BOOTSTRAP_SERVERS = aws_msk_cluster.main.bootstrap_brokers_tls
    KAFKA_CLUSTER_NAME     = aws_msk_cluster.main.cluster_name
    KAFKA_SECURITY_PROTOCOL = var.client_broker_encryption == "TLS" ? "SSL" : "PLAINTEXT"
    KAFKA_AUTO_OFFSET_RESET = "earliest"
    KAFKA_ENABLE_AUTO_COMMIT = "true"
    KAFKA_SESSION_TIMEOUT_MS = "30000"
    KAFKA_REQUEST_TIMEOUT_MS = "40000"
  }
}
