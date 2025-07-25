# MSK Module for E-Commerce Analytics Platform
# Provides managed Apache Kafka service for streaming data

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# KMS Key for MSK encryption
resource "aws_kms_key" "msk" {
  description             = "MSK encryption key for ${var.cluster_name}"
  deletion_window_in_days = var.kms_key_deletion_window

  tags = merge(var.tags, {
    Name = "${var.cluster_name}-msk-kms-key"
    Type = "KMSKey"
  })
}

resource "aws_kms_alias" "msk" {
  name          = "alias/${var.cluster_name}-msk-encryption"
  target_key_id = aws_kms_key.msk.key_id
}

# CloudWatch Log Group for MSK logs
resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${var.cluster_name}"
  retention_in_days = var.cloudwatch_log_retention_days

  tags = merge(var.tags, {
    Name = "${var.cluster_name}-msk-log-group"
    Type = "LogGroup"
  })
}

# MSK Configuration
resource "aws_msk_configuration" "main" {
  kafka_versions = [var.kafka_version]
  name           = "${var.cluster_name}-config"
  description    = "MSK configuration for ${var.cluster_name}"

  server_properties = <<PROPERTIES
auto.create.topics.enable=${var.auto_create_topics_enable}
default.replication.factor=${var.default_replication_factor}
min.insync.replicas=${var.min_insync_replicas}
num.io.threads=${var.num_io_threads}
num.network.threads=${var.num_network_threads}
num.partitions=${var.num_partitions}
num.replica.fetchers=${var.num_replica_fetchers}
replica.lag.time.max.ms=${var.replica_lag_time_max_ms}
socket.receive.buffer.bytes=${var.socket_receive_buffer_bytes}
socket.request.max.bytes=${var.socket_request_max_bytes}
socket.send.buffer.bytes=${var.socket_send_buffer_bytes}
unclean.leader.election.enable=${var.unclean_leader_election_enable}
log.retention.hours=${var.log_retention_hours}
log.retention.bytes=${var.log_retention_bytes}
log.segment.bytes=${var.log_segment_bytes}
log.cleanup.policy=${var.log_cleanup_policy}
compression.type=${var.compression_type}
PROPERTIES

  tags = var.tags
}

# MSK Cluster
resource "aws_msk_cluster" "main" {
  cluster_name           = var.cluster_name
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.number_of_broker_nodes

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    ebs_volume_size = var.broker_ebs_volume_size
    client_subnets  = var.subnet_ids
    security_groups = [var.security_group_id]

    storage_info {
      ebs_storage_info {
        volume_size = var.broker_ebs_volume_size
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }

  # Encryption in transit
  encryption_info {
    encryption_at_rest_kms_key_id = aws_kms_key.msk.key_id

    encryption_in_transit {
      client_broker = var.client_broker_encryption
      in_cluster    = var.in_cluster_encryption
    }
  }

  # Enhanced monitoring
  enhanced_monitoring = var.enhanced_monitoring

  # OpenMonitoring with Prometheus
  dynamic "open_monitoring" {
    for_each = var.enable_prometheus_monitoring ? [1] : []
    content {
      prometheus {
        jmx_exporter {
          enabled_in_broker = var.prometheus_jmx_exporter_enabled
        }
        node_exporter {
          enabled_in_broker = var.prometheus_node_exporter_enabled
        }
      }
    }
  }

  # Logging
  logging_info {
    broker_logs {
      dynamic "cloudwatch_logs" {
        for_each = var.enable_cloudwatch_logs ? [1] : []
        content {
          enabled   = true
          log_group = aws_cloudwatch_log_group.msk.name
        }
      }

      dynamic "firehose" {
        for_each = var.enable_firehose_logs ? [1] : []
        content {
          enabled         = true
          delivery_stream = var.firehose_delivery_stream_name
        }
      }

      dynamic "s3" {
        for_each = var.enable_s3_logs ? [1] : []
        content {
          enabled = true
          bucket  = var.s3_logs_bucket_name
          prefix  = var.s3_logs_prefix
        }
      }
    }
  }

  # Client authentication
  dynamic "client_authentication" {
    for_each = var.enable_client_authentication ? [1] : []
    content {
      dynamic "sasl" {
        for_each = var.enable_sasl_authentication ? [1] : []
        content {
          scram = var.enable_sasl_scram
          iam   = var.enable_sasl_iam
        }
      }

      dynamic "tls" {
        for_each = var.enable_tls_authentication ? [1] : []
        content {
          certificate_authority_arns = var.certificate_authority_arns
        }
      }

      unauthenticated = var.enable_unauthenticated_access
    }
  }

  tags = merge(var.tags, {
    Name = var.cluster_name
    Type = "MSKCluster"
  })

  timeouts {
    create = "120m"
    update = "120m"
    delete = "120m"
  }
}

# CloudWatch Alarms for MSK monitoring
resource "aws_cloudwatch_metric_alarm" "kafka_cpu_utilization" {
  alarm_name          = "${var.cluster_name}-cpu-utilization"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "CpuIdle"
  namespace           = "AWS/Kafka"
  period              = "300"
  statistic           = "Average"
  threshold           = "20"  # Alert when CPU idle is less than 20%
  alarm_description   = "This metric monitors Kafka CPU utilization"
  treat_missing_data  = "breaching"

  dimensions = {
    "Cluster Name" = aws_msk_cluster.main.cluster_name
  }

  alarm_actions = var.alarm_sns_topic_arn != null ? [var.alarm_sns_topic_arn] : []

  tags = merge(var.tags, {
    Name = "${var.cluster_name}-cpu-alarm"
    Type = "CloudWatchAlarm"
  })
}

resource "aws_cloudwatch_metric_alarm" "kafka_disk_usage" {
  alarm_name          = "${var.cluster_name}-disk-usage"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "KafkaDataLogsDiskUsed"
  namespace           = "AWS/Kafka"
  period              = "300"
  statistic           = "Average"
  threshold           = "85"  # Alert when disk usage exceeds 85%
  alarm_description   = "This metric monitors Kafka disk usage"
  treat_missing_data  = "breaching"

  dimensions = {
    "Cluster Name" = aws_msk_cluster.main.cluster_name
  }

  alarm_actions = var.alarm_sns_topic_arn != null ? [var.alarm_sns_topic_arn] : []

  tags = merge(var.tags, {
    Name = "${var.cluster_name}-disk-alarm"
    Type = "CloudWatchAlarm"
  })
}

resource "aws_cloudwatch_metric_alarm" "kafka_memory_usage" {
  alarm_name          = "${var.cluster_name}-memory-usage"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "MemoryUsed"
  namespace           = "AWS/Kafka"
  period              = "300"
  statistic           = "Average"
  threshold           = "85"  # Alert when memory usage exceeds 85%
  alarm_description   = "This metric monitors Kafka memory usage"
  treat_missing_data  = "breaching"

  dimensions = {
    "Cluster Name" = aws_msk_cluster.main.cluster_name
  }

  alarm_actions = var.alarm_sns_topic_arn != null ? [var.alarm_sns_topic_arn] : []

  tags = merge(var.tags, {
    Name = "${var.cluster_name}-memory-alarm"
    Type = "CloudWatchAlarm"
  })
}

# Create Kafka topics (optional)
# Note: This would typically be done by applications or configuration management
# but we can create essential topics here

locals {
  topics = [
    {
      name               = "transactions"
      partitions         = var.transactions_topic_partitions
      replication_factor = var.default_replication_factor
      config = {
        "cleanup.policy" = "delete"
        "retention.ms"   = "604800000"  # 7 days
      }
    },
    {
      name               = "user-events"
      partitions         = var.user_events_topic_partitions
      replication_factor = var.default_replication_factor
      config = {
        "cleanup.policy" = "delete"
        "retention.ms"   = "259200000"  # 3 days
      }
    },
    {
      name               = "product-updates"
      partitions         = var.product_updates_topic_partitions
      replication_factor = var.default_replication_factor
      config = {
        "cleanup.policy" = "compact"
        "retention.ms"   = "2592000000"  # 30 days
      }
    },
    {
      name               = "fraud-alerts"
      partitions         = var.fraud_alerts_topic_partitions
      replication_factor = var.default_replication_factor
      config = {
        "cleanup.policy" = "delete"
        "retention.ms"   = "2592000000"  # 30 days
      }
    }
  ]
}

# Output topics configuration for applications to use
resource "local_file" "topics_config" {
  count = var.create_topics_config ? 1 : 0

  filename = "${path.module}/topics-config.json"
  content = jsonencode({
    cluster_name = aws_msk_cluster.main.cluster_name
    bootstrap_brokers = aws_msk_cluster.main.bootstrap_brokers
    bootstrap_brokers_sasl_scram = aws_msk_cluster.main.bootstrap_brokers_sasl_scram
    bootstrap_brokers_sasl_iam = aws_msk_cluster.main.bootstrap_brokers_sasl_iam
    bootstrap_brokers_tls = aws_msk_cluster.main.bootstrap_brokers_tls
    topics = local.topics
  })
}
