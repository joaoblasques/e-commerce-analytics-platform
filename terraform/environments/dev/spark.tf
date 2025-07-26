# EMR Spark Cluster for Development Environment

# Data sources
data "aws_s3_bucket" "data_lake" {
  bucket = "${var.project_name}-${var.environment}-data-lake"
}

# EMR Cluster Module
module "emr_cluster" {
  source = "../../modules/emr"

  # Environment Configuration
  environment  = var.environment
  project_name = var.project_name

  # Network Configuration
  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids

  # Security Groups
  emr_security_group_id                = module.security_groups.emr_security_group_id
  emr_managed_master_security_group_id = module.security_groups.emr_master_security_group_id
  emr_managed_slave_security_group_id  = module.security_groups.emr_slave_security_group_id
  additional_security_group_ids        = [module.security_groups.monitoring_security_group_id]

  # Instance Configuration (Development - smaller instances)
  master_instance_type = "m5.large"
  core_instance_type   = "m5.large"
  core_instance_count  = 2
  task_instance_type   = "m5.large"
  task_instance_count  = 1
  max_task_instances   = 5

  # Auto Scaling Configuration
  auto_scaling_enabled     = true
  auto_scaling_min_capacity = 1
  auto_scaling_max_capacity = 8
  scale_up_cooldown        = 300
  scale_down_cooldown      = 600

  # Spot Instance Configuration (Cost Optimization)
  spot_instance_enabled   = true
  spot_bid_percentage     = 60
  spot_timeout_action     = "TERMINATE_CLUSTER"

  # Storage Configuration
  master_ebs_size = 50
  core_ebs_size   = 100
  task_ebs_size   = 50
  ebs_volume_type = "gp3"

  # Logging and Storage
  log_bucket = module.s3.data_lake_bucket_name
  kms_key_id = module.s3.kms_key_arn

  # Security Configuration
  encryption_at_rest_enabled    = true
  encryption_in_transit_enabled = true

  # Monitoring
  enable_debugging              = true
  enable_cloudwatch_monitoring  = true

  # Cost Optimization
  terminate_unhealthy_nodes = true
  idle_timeout             = 3600  # 1 hour for dev
  scale_down_behavior      = "TERMINATE_AT_TASK_COMPLETION"

  # Applications
  applications = ["Spark", "Hadoop", "Hive", "Livy", "JupyterHub", "Zeppelin"]

  # Bootstrap Actions
  bootstrap_actions = [
    {
      name = "Install Python Dependencies"
      path = "s3://${module.s3.data_lake_bucket_name}/bootstrap/install-python-deps.sh"
      args = ["--environment", var.environment, "--project", var.project_name]
    },
    {
      name = "Configure Monitoring"
      path = "s3://${module.s3.data_lake_bucket_name}/bootstrap/setup-monitoring.sh"
      args = ["--prometheus-endpoint", "http://prometheus.monitoring.svc.cluster.local:9090"]
    }
  ]

  # Tagging
  tags = local.common_tags
  additional_tags = {
    Service   = "EMR"
    Component = "SparkCluster"
    Purpose   = "DataProcessing"
  }

  depends_on = [
    module.vpc,
    module.security_groups,
    module.s3
  ]
}

# CloudWatch Log Group for EMR Spark Applications
resource "aws_cloudwatch_log_group" "emr_spark_logs" {
  name              = "/aws/emr/${var.project_name}-${var.environment}/spark-applications"
  retention_in_days = var.environment == "prod" ? 30 : 7

  tags = merge(local.common_tags, {
    Name    = "${var.project_name}-${var.environment}-emr-spark-logs"
    Service = "EMR"
  })
}

# CloudWatch Log Group for EMR Cluster Logs
resource "aws_cloudwatch_log_group" "emr_cluster_logs" {
  name              = "/aws/emr/${var.project_name}-${var.environment}/cluster"
  retention_in_days = var.environment == "prod" ? 30 : 7

  tags = merge(local.common_tags, {
    Name    = "${var.project_name}-${var.environment}-emr-cluster-logs"
    Service = "EMR"
  })
}

# S3 objects for bootstrap scripts
resource "aws_s3_object" "python_deps_bootstrap" {
  bucket = module.s3.data_lake_bucket_name
  key    = "bootstrap/install-python-deps.sh"
  content = templatefile("${path.module}/templates/install-python-deps.sh.tpl", {
    environment  = var.environment
    project_name = var.project_name
  })
  content_type = "text/plain"

  tags = local.common_tags
}

resource "aws_s3_object" "monitoring_bootstrap" {
  bucket = module.s3.data_lake_bucket_name
  key    = "bootstrap/setup-monitoring.sh"
  content = templatefile("${path.module}/templates/setup-monitoring.sh.tpl", {
    environment       = var.environment
    project_name      = var.project_name
    prometheus_url    = "http://prometheus.monitoring.svc.cluster.local:9090"
    grafana_url       = "http://grafana.monitoring.svc.cluster.local:3000"
  })
  content_type = "text/plain"

  tags = local.common_tags
}

# Upload sample Spark jobs to S3
resource "aws_s3_object" "sample_spark_jobs" {
  for_each = fileset("${path.module}/spark-jobs/", "*.py")

  bucket = module.s3.data_lake_bucket_name
  key    = "spark-jobs/${each.value}"
  source = "${path.module}/spark-jobs/${each.value}"
  etag   = filemd5("${path.module}/spark-jobs/${each.value}")

  tags = local.common_tags
}

# CloudWatch Alarms for EMR Cluster Monitoring
resource "aws_cloudwatch_metric_alarm" "emr_cluster_health" {
  alarm_name          = "${var.project_name}-${var.environment}-emr-cluster-health"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "IsIdle"
  namespace           = "AWS/ElasticMapReduce"
  period              = "300"
  statistic           = "Average"
  threshold           = "1"
  alarm_description   = "This metric monitors EMR cluster health status"
  alarm_actions       = [aws_sns_topic.emr_alerts.arn]

  dimensions = {
    JobFlowId = module.emr_cluster.cluster_id
  }

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "emr_cluster_cost" {
  alarm_name          = "${var.project_name}-${var.environment}-emr-high-cost"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "TotalCost"
  namespace           = "AWS/ElasticMapReduce"
  period              = "3600"  # 1 hour
  statistic           = "Sum"
  threshold           = var.environment == "dev" ? "50" : "200"  # USD
  alarm_description   = "EMR cluster cost is exceeding threshold"
  alarm_actions       = [aws_sns_topic.emr_alerts.arn]

  dimensions = {
    JobFlowId = module.emr_cluster.cluster_id
  }

  tags = local.common_tags
}

# SNS Topic for EMR Alerts
resource "aws_sns_topic" "emr_alerts" {
  name = "${var.project_name}-${var.environment}-emr-alerts"

  tags = local.common_tags
}

resource "aws_sns_topic_subscription" "emr_alerts_email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.emr_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Output EMR cluster information
output "emr_cluster_info" {
  description = "EMR cluster configuration and connection information"
  value = {
    cluster_id         = module.emr_cluster.cluster_id
    cluster_name       = module.emr_cluster.cluster_name
    master_public_dns  = module.emr_cluster.master_public_dns
    connection_info    = module.emr_cluster.connection_info
    monitoring_info    = module.emr_cluster.monitoring_info
    configuration      = module.emr_cluster.cluster_configuration
  }
}
