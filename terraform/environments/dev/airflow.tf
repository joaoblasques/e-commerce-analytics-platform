# Apache Airflow for Development Environment

# Random password for Airflow database
resource "random_password" "airflow_db_password" {
  length  = 16
  special = true
}

# Random password for Airflow admin user
resource "random_password" "airflow_admin_password" {
  length  = 16
  special = true
}

# Apache Airflow Module
module "airflow" {
  source = "../../modules/airflow"

  # Environment Configuration
  environment  = var.environment
  project_name = var.project_name

  # Network Configuration
  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids

  # Security Groups
  airflow_security_group_id = module.security_groups.airflow_security_group_id
  rds_security_group_id     = module.security_groups.rds_security_group_id
  redis_security_group_id   = module.security_groups.redis_security_group_id

  # Database Configuration (Development - smaller instance)
  airflow_db_instance_class    = "db.t3.micro"
  airflow_db_allocated_storage = 20
  airflow_db_username          = "airflow"
  airflow_db_password          = random_password.airflow_db_password.result

  # Redis Configuration (Development - smaller instance)
  redis_node_type        = "cache.t3.micro"
  redis_num_cache_nodes  = 1
  redis_parameter_group_name = "default.redis7"
  redis_port            = 6379

  # S3 Configuration
  airflow_s3_bucket = module.s3.data_lake_bucket_name
  kms_key_id       = module.s3.kms_key_arn

  # Kubernetes Configuration
  kubernetes_namespace = "airflow"
  airflow_image_tag   = "2.8.1-python3.9"
  airflow_executor    = "CeleryExecutor"  # Use Celery for scalability

  # Airflow Admin Configuration
  airflow_admin_username  = "admin"
  airflow_admin_password  = random_password.airflow_admin_password.result
  airflow_admin_email     = var.admin_email != "" ? var.admin_email : "admin@ecap.local"
  airflow_admin_firstname = "ECAP"
  airflow_admin_lastname  = "Administrator"

  # Scaling Configuration (Development - smaller scale)
  webserver_replicas   = 1
  scheduler_replicas   = 1
  worker_replicas      = 2
  worker_max_replicas  = 5

  # Resource Configuration (Development - conservative resources)
  webserver_resources = {
    requests = {
      cpu    = "100m"
      memory = "256Mi"
    }
    limits = {
      cpu    = "500m"
      memory = "1Gi"
    }
  }

  scheduler_resources = {
    requests = {
      cpu    = "100m"
      memory = "256Mi"
    }
    limits = {
      cpu    = "500m"
      memory = "1Gi"
    }
  }

  worker_resources = {
    requests = {
      cpu    = "200m"
      memory = "512Mi"
    }
    limits = {
      cpu    = "1000m"
      memory = "2Gi"
    }
  }

  # Storage Configuration
  logs_volume_size = "10Gi"
  dags_volume_size = "5Gi"

  # Monitoring Configuration
  enable_prometheus_monitoring = true
  enable_flower               = true
  flower_port                = 5555

  # Load Balancer Configuration
  enable_load_balancer = true
  load_balancer_type  = "application"

  # EMR Integration
  emr_cluster_id          = module.emr_cluster.cluster_id
  emr_service_role_arn    = module.emr_cluster.service_role_arn
  emr_instance_profile_arn = module.emr_cluster.instance_profile_arn

  # Tagging
  tags = local.common_tags
  additional_tags = {
    Service   = "Airflow"
    Component = "Orchestration"
    Purpose   = "WorkflowManagement"
  }

  depends_on = [
    module.vpc,
    module.security_groups,
    module.s3,
    module.emr_cluster
  ]
}

# Store Airflow passwords in AWS Secrets Manager
resource "aws_secretsmanager_secret" "airflow_passwords" {
  name        = "${var.project_name}-${var.environment}-airflow-passwords"
  description = "Airflow passwords for ${var.project_name} ${var.environment} environment"

  tags = merge(local.common_tags, {
    Name    = "${var.project_name}-${var.environment}-airflow-passwords"
    Service = "Airflow"
  })
}

resource "aws_secretsmanager_secret_version" "airflow_passwords" {
  secret_id = aws_secretsmanager_secret.airflow_passwords.id
  secret_string = jsonencode({
    db_password    = random_password.airflow_db_password.result
    admin_password = random_password.airflow_admin_password.result
    admin_username = "admin"
    db_username    = "airflow"
  })
}

# S3 objects for Airflow DAGs
resource "aws_s3_object" "airflow_dags" {
  for_each = fileset("${path.module}/airflow-dags/", "*.py")

  bucket = module.s3.data_lake_bucket_name
  key    = "airflow/dags/${each.value}"
  source = "${path.module}/airflow-dags/${each.value}"
  etag   = filemd5("${path.module}/airflow-dags/${each.value}")

  tags = local.common_tags
}

# S3 objects for Airflow plugins
resource "aws_s3_object" "airflow_plugins" {
  for_each = fileset("${path.module}/airflow-plugins/", "*.py")

  bucket = module.s3.data_lake_bucket_name
  key    = "airflow/plugins/${each.value}"
  source = "${path.module}/airflow-plugins/${each.value}"
  etag   = filemd5("${path.module}/airflow-plugins/${each.value}")

  tags = local.common_tags
}

# CloudWatch Log Groups for Airflow
resource "aws_cloudwatch_log_group" "airflow_webserver" {
  name              = "/aws/airflow/${var.project_name}-${var.environment}/webserver"
  retention_in_days = var.environment == "prod" ? 30 : 7

  tags = merge(local.common_tags, {
    Name    = "${var.project_name}-${var.environment}-airflow-webserver-logs"
    Service = "Airflow"
  })
}

resource "aws_cloudwatch_log_group" "airflow_scheduler" {
  name              = "/aws/airflow/${var.project_name}-${var.environment}/scheduler"
  retention_in_days = var.environment == "prod" ? 30 : 7

  tags = merge(local.common_tags, {
    Name    = "${var.project_name}-${var.environment}-airflow-scheduler-logs"
    Service = "Airflow"
  })
}

resource "aws_cloudwatch_log_group" "airflow_workers" {
  name              = "/aws/airflow/${var.project_name}-${var.environment}/workers"
  retention_in_days = var.environment == "prod" ? 30 : 7

  tags = merge(local.common_tags, {
    Name    = "${var.project_name}-${var.environment}-airflow-workers-logs"
    Service = "Airflow"
  })
}

# CloudWatch Alarms for Airflow Monitoring
resource "aws_cloudwatch_metric_alarm" "airflow_failed_tasks" {
  alarm_name          = "${var.project_name}-${var.environment}-airflow-failed-tasks"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "task_failed"
  namespace           = "Airflow"
  period              = "300"
  statistic           = "Sum"
  threshold           = "5"
  alarm_description   = "This metric monitors failed Airflow tasks"
  alarm_actions       = [aws_sns_topic.airflow_alerts.arn]

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "airflow_db_connections" {
  alarm_name          = "${var.project_name}-${var.environment}-airflow-db-connections"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "DatabaseConnections"
  namespace           = "AWS/RDS"
  period              = "300"
  statistic           = "Average"
  threshold           = "80"
  alarm_description   = "This metric monitors Airflow database connections"
  alarm_actions       = [aws_sns_topic.airflow_alerts.arn]

  dimensions = {
    DBInstanceIdentifier = module.airflow.rds_endpoint
  }

  tags = local.common_tags
}

# SNS Topic for Airflow Alerts
resource "aws_sns_topic" "airflow_alerts" {
  name = "${var.project_name}-${var.environment}-airflow-alerts"

  tags = local.common_tags
}

resource "aws_sns_topic_subscription" "airflow_alerts_email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.airflow_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Output Airflow information
output "airflow_info" {
  description = "Airflow deployment configuration and connection information"
  value = {
    webserver_url            = module.airflow.connection_info.webserver_url
    flower_url              = module.airflow.connection_info.flower_url
    database_endpoint       = module.airflow.rds_endpoint
    redis_endpoint          = module.airflow.redis_endpoint
    s3_bucket              = module.airflow.s3_bucket_name
    kubernetes_namespace    = module.airflow.kubernetes_namespace
    configuration          = module.airflow.airflow_configuration
    resource_details       = module.airflow.resource_details
    monitoring_integration = module.airflow.monitoring_integration
  }
  sensitive = true
}

# Output sensitive information separately
output "airflow_credentials" {
  description = "Airflow admin credentials (sensitive)"
  value = {
    admin_username = "admin"
    admin_password = random_password.airflow_admin_password.result
    secret_arn     = aws_secretsmanager_secret.airflow_passwords.arn
  }
  sensitive = true
}
