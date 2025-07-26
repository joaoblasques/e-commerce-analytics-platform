# EMR Cluster Module - Main Configuration

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Data sources
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# IAM Role for EMR Service
resource "aws_iam_role" "emr_service_role" {
  name = "${var.project_name}-${var.environment}-emr-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "elasticmapreduce.amazonaws.com"
        }
      }
    ]
  })

  tags = merge(var.tags, var.additional_tags, {
    Name = "${var.project_name}-${var.environment}-emr-service-role"
  })
}

resource "aws_iam_role_policy_attachment" "emr_service_role_policy" {
  role       = aws_iam_role.emr_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

# IAM Role for EMR EC2 Instances
resource "aws_iam_role" "emr_ec2_instance_role" {
  name = "${var.project_name}-${var.environment}-emr-ec2-instance-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = merge(var.tags, var.additional_tags, {
    Name = "${var.project_name}-${var.environment}-emr-ec2-instance-role"
  })
}

resource "aws_iam_role_policy_attachment" "emr_ec2_instance_profile_policy" {
  role       = aws_iam_role.emr_ec2_instance_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

# Custom policy for S3 access and additional permissions
resource "aws_iam_role_policy" "emr_ec2_custom_policy" {
  name = "${var.project_name}-${var.environment}-emr-ec2-custom-policy"
  role = aws_iam_role.emr_ec2_instance_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "${var.log_bucket}/*",
          var.log_bucket,
          "arn:aws:s3:::${var.project_name}-${var.environment}-data-lake",
          "arn:aws:s3:::${var.project_name}-${var.environment}-data-lake/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData",
          "cloudwatch:GetMetricStatistics",
          "cloudwatch:ListMetrics",
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = [var.kms_key_id]
      }
    ]
  })
}

# IAM Instance Profile
resource "aws_iam_instance_profile" "emr_ec2_instance_profile" {
  name = "${var.project_name}-${var.environment}-emr-ec2-instance-profile"
  role = aws_iam_role.emr_ec2_instance_role.name

  tags = merge(var.tags, var.additional_tags, {
    Name = "${var.project_name}-${var.environment}-emr-ec2-instance-profile"
  })
}

# IAM Role for Auto Scaling
resource "aws_iam_role" "emr_autoscaling_role" {
  count = var.auto_scaling_enabled ? 1 : 0
  name  = "${var.project_name}-${var.environment}-emr-autoscaling-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "application-autoscaling.amazonaws.com"
        }
      },
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "elasticmapreduce.amazonaws.com"
        }
      }
    ]
  })

  tags = merge(var.tags, var.additional_tags, {
    Name = "${var.project_name}-${var.environment}-emr-autoscaling-role"
  })
}

resource "aws_iam_role_policy_attachment" "emr_autoscaling_role_policy" {
  count      = var.auto_scaling_enabled ? 1 : 0
  role       = aws_iam_role.emr_autoscaling_role[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforAutoScalingRole"
}

# Security Configuration for Encryption
resource "aws_emr_security_configuration" "emr_security_config" {
  count = var.encryption_at_rest_enabled || var.encryption_in_transit_enabled ? 1 : 0
  name  = "${var.project_name}-${var.environment}-emr-security-config"

  configuration = jsonencode({
    EncryptionConfiguration = {
      AtRestEncryptionConfiguration = var.encryption_at_rest_enabled ? {
        S3EncryptionConfiguration = {
          EncryptionMode = "SSE-KMS"
          AwsKmsKey      = var.kms_key_id
        }
        LocalDiskEncryptionConfiguration = {
          EncryptionKeyProviderType = "AwsKms"
          AwsKmsKey                = var.kms_key_id
        }
      } : null

      InTransitEncryptionConfiguration = var.encryption_in_transit_enabled ? {
        TLSCertificateConfiguration = {
          CertificateProviderType = "PEM"
          S3Object               = "${var.log_bucket}/certificates/"
        }
      } : null
    }
  })
}

# Bootstrap script for cluster configuration
resource "aws_s3_object" "bootstrap_script" {
  bucket = var.log_bucket
  key    = "bootstrap/emr-bootstrap.sh"
  content = templatefile("${path.module}/templates/bootstrap.sh.tpl", {
    environment    = var.environment
    project_name   = var.project_name
    log_bucket     = var.log_bucket
    kms_key_id     = var.kms_key_id
  })
  content_type = "text/plain"

  tags = merge(var.tags, var.additional_tags)
}

# EMR Cluster
resource "aws_emr_cluster" "cluster" {
  name          = "${var.project_name}-${var.environment}-spark-cluster"
  release_label = var.release_label
  applications  = var.applications

  # Service role
  service_role = aws_iam_role.emr_service_role.arn

  # Security configuration
  security_configuration = var.encryption_at_rest_enabled || var.encryption_in_transit_enabled ? aws_emr_security_configuration.emr_security_config[0].name : null

  # Logging
  log_uri = "s3://${var.log_bucket}/emr-logs/"

  # Auto-termination
  auto_termination_policy {
    idle_timeout = var.idle_timeout
  }

  # Master node configuration
  master_instance_group {
    instance_type  = var.master_instance_type
    instance_count = 1
    name           = "master"

    ebs_config {
      size                 = var.master_ebs_size
      type                 = var.ebs_volume_type
      volumes_per_instance = 1
    }
  }

  # Core nodes configuration
  core_instance_group {
    instance_type  = var.core_instance_type
    instance_count = var.core_instance_count
    name           = "core"

    ebs_config {
      size                 = var.core_ebs_size
      type                 = var.ebs_volume_type
      volumes_per_instance = 1
    }

    # Auto scaling for core nodes
    dynamic "autoscaling_policy" {
      for_each = var.auto_scaling_enabled ? [1] : []
      content {
        constraints {
          min_capacity = var.auto_scaling_min_capacity
          max_capacity = var.auto_scaling_max_capacity
        }

        rules {
          name        = "ScaleOutMemoryPercentage"
          description = "Scale out based on memory utilization"
          action {
            market                 = "ON_DEMAND"
            simple_scaling_policy_configuration {
              adjustment_type          = "CHANGE_IN_CAPACITY"
              scaling_adjustment       = 1
              cool_down               = var.scale_up_cooldown
            }
          }
          trigger {
            cloud_watch_alarm_definition {
              comparison_operator = "GREATER_THAN"
              evaluation_periods  = 1
              metric_name        = "MemoryPercentage"
              namespace          = "AWS/ElasticMapReduce"
              period             = 300
              statistic          = "AVERAGE"
              threshold          = 75.0
              unit               = "PERCENT"
              dimensions = {
                JobFlowId = aws_emr_cluster.cluster.id
              }
            }
          }
        }

        rules {
          name        = "ScaleInMemoryPercentage"
          description = "Scale in based on memory utilization"
          action {
            market                 = "ON_DEMAND"
            simple_scaling_policy_configuration {
              adjustment_type          = "CHANGE_IN_CAPACITY"
              scaling_adjustment       = -1
              cool_down               = var.scale_down_cooldown
            }
          }
          trigger {
            cloud_watch_alarm_definition {
              comparison_operator = "LESS_THAN"
              evaluation_periods  = 1
              metric_name        = "MemoryPercentage"
              namespace          = "AWS/ElasticMapReduce"
              period             = 300
              statistic          = "AVERAGE"
              threshold          = 25.0
              unit               = "PERCENT"
              dimensions = {
                JobFlowId = aws_emr_cluster.cluster.id
              }
            }
          }
        }
      }
    }
  }

  # EC2 attributes
  ec2_attributes {
    subnet_id                         = var.private_subnet_ids[0]
    emr_managed_master_security_group = var.emr_managed_master_security_group_id
    emr_managed_slave_security_group  = var.emr_managed_slave_security_group_id
    additional_master_security_groups = join(",", var.additional_security_group_ids)
    additional_slave_security_groups  = join(",", var.additional_security_group_ids)
    instance_profile                  = aws_iam_instance_profile.emr_ec2_instance_profile.arn
  }

  # Bootstrap actions
  dynamic "bootstrap_action" {
    for_each = concat(var.bootstrap_actions, [{
      name = "Configure Cluster"
      path = "s3://${var.log_bucket}/bootstrap/emr-bootstrap.sh"
      args = []
    }])

    content {
      name = bootstrap_action.value.name
      path = bootstrap_action.value.path
      args = bootstrap_action.value.args
    }
  }

  # Spark configuration
  configurations_json = jsonencode([
    {
      Classification = "spark-defaults"
      Properties = {
        "spark.dynamicAllocation.enabled"                    = "true"
        "spark.dynamicAllocation.minExecutors"              = "1"
        "spark.dynamicAllocation.maxExecutors"              = var.max_task_instances
        "spark.dynamicAllocation.initialExecutors"          = var.task_instance_count
        "spark.dynamicAllocation.executorIdleTimeout"       = "60s"
        "spark.dynamicAllocation.cachedExecutorIdleTimeout" = "300s"
        "spark.sql.adaptive.enabled"                        = "true"
        "spark.sql.adaptive.coalescePartitions.enabled"     = "true"
        "spark.sql.adaptive.skewJoin.enabled"               = "true"
        "spark.serializer"                                   = "org.apache.spark.serializer.KryoSerializer"
        "spark.sql.execution.arrow.pyspark.enabled"         = "true"
        "spark.sql.adaptive.advisoryPartitionSizeInBytes"   = "256MB"
        "spark.eventLog.enabled"                             = "true"
        "spark.eventLog.dir"                                 = "s3://${var.log_bucket}/spark-event-logs/"
        "spark.history.fs.logDirectory"                     = "s3://${var.log_bucket}/spark-event-logs/"
      }
    },
    {
      Classification = "spark-env"
      Properties = {}
      Configurations = [
        {
          Classification = "export"
          Properties = {
            "PYSPARK_PYTHON" = "/usr/bin/python3"
          }
        }
      ]
    },
    {
      Classification = "yarn-site"
      Properties = {
        "yarn.resourcemanager.scheduler.class"                                   = "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler"
        "yarn.scheduler.capacity.resource-calculator"                           = "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
        "yarn.log-aggregation-enable"                                           = "true"
        "yarn.log-aggregation.retain-seconds"                                   = "604800"
        "yarn.nodemanager.remote-app-log-dir"                                   = "s3://${var.log_bucket}/yarn-logs/"
        "yarn.nodemanager.vmem-check-enabled"                                   = "false"
        "yarn.nodemanager.pmem-check-enabled"                                   = "false"
      }
    },
    {
      Classification = "hadoop-env"
      Properties = {}
      Configurations = [
        {
          Classification = "export"
          Properties = {
            "HADOOP_DATANODE_HEAPSIZE" = "2048"
            "HADOOP_NAMENODE_HEAPSIZE" = "2048"
          }
        }
      ]
    }
  ])

  # Enable debugging and monitoring
  step_concurrency_level = 10
  keep_job_flow_alive_when_no_steps = true

  tags = merge(var.tags, var.additional_tags, {
    Name        = "${var.project_name}-${var.environment}-spark-cluster"
    Environment = var.environment
    Service     = "EMR"
    Component   = "SparkCluster"
  })

  depends_on = [
    aws_iam_role_policy_attachment.emr_service_role_policy,
    aws_iam_role_policy_attachment.emr_ec2_instance_profile_policy,
    aws_s3_object.bootstrap_script
  ]
}

# Task instances with spot pricing
resource "aws_emr_instance_group" "task_group" {
  count      = var.spot_instance_enabled ? 1 : 0
  cluster_id = aws_emr_cluster.cluster.id
  name       = "task"

  instance_type  = var.task_instance_type
  instance_count = var.task_instance_count

  bid_price = var.spot_bid_percentage > 0 ? "${var.spot_bid_percentage}%" : null

  ebs_config {
    size                 = var.task_ebs_size
    type                 = var.ebs_volume_type
    volumes_per_instance = 1
  }

  # Auto scaling for task nodes
  dynamic "autoscaling_policy" {
    for_each = var.auto_scaling_enabled ? [1] : []
    content {
      constraints {
        min_capacity = 0
        max_capacity = var.max_task_instances
      }

      rules {
        name        = "ScaleOutContainerPending"
        description = "Scale out based on pending containers"
        action {
          market                 = "SPOT"
          simple_scaling_policy_configuration {
            adjustment_type          = "CHANGE_IN_CAPACITY"
            scaling_adjustment       = 2
            cool_down               = var.scale_up_cooldown
          }
        }
        trigger {
          cloud_watch_alarm_definition {
            comparison_operator = "GREATER_THAN"
            evaluation_periods  = 1
            metric_name        = "ContainerPending"
            namespace          = "AWS/ElasticMapReduce"
            period             = 300
            statistic          = "AVERAGE"
            threshold          = 0.0
            unit               = "COUNT"
            dimensions = {
              JobFlowId = aws_emr_cluster.cluster.id
            }
          }
        }
      }

      rules {
        name        = "ScaleInMemoryPercentage"
        description = "Scale in based on low memory utilization"
        action {
          market                 = "SPOT"
          simple_scaling_policy_configuration {
            adjustment_type          = "CHANGE_IN_CAPACITY"
            scaling_adjustment       = -1
            cool_down               = var.scale_down_cooldown
          }
        }
        trigger {
          cloud_watch_alarm_definition {
            comparison_operator = "LESS_THAN"
            evaluation_periods  = 2
            metric_name        = "MemoryPercentage"
            namespace          = "AWS/ElasticMapReduce"
            period             = 300
            statistic          = "AVERAGE"
            threshold          = 15.0
            unit               = "PERCENT"
            dimensions = {
              JobFlowId = aws_emr_cluster.cluster.id
            }
          }
        }
      }
    }
  }
}

# CloudWatch Alarms for Monitoring
resource "aws_cloudwatch_metric_alarm" "cluster_memory_utilization" {
  count = var.enable_cloudwatch_monitoring ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-emr-memory-utilization"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "MemoryPercentage"
  namespace           = "AWS/ElasticMapReduce"
  period              = "300"
  statistic           = "Average"
  threshold           = "80"
  alarm_description   = "This metric monitors EMR cluster memory utilization"
  insufficient_data_actions = []

  dimensions = {
    JobFlowId = aws_emr_cluster.cluster.id
  }

  tags = merge(var.tags, var.additional_tags)
}

resource "aws_cloudwatch_metric_alarm" "cluster_apps_failed" {
  count = var.enable_cloudwatch_monitoring ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-emr-apps-failed"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "AppsSubmitted"
  namespace           = "AWS/ElasticMapReduce"
  period              = "300"
  statistic           = "Sum"
  threshold           = "0"
  alarm_description   = "This metric monitors failed applications in EMR cluster"
  insufficient_data_actions = []

  dimensions = {
    JobFlowId = aws_emr_cluster.cluster.id
  }

  tags = merge(var.tags, var.additional_tags)
}
