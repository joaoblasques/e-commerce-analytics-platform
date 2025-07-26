# Apache Airflow Module Outputs

# RDS Database Outputs
output "rds_endpoint" {
  description = "RDS endpoint for Airflow metadata database"
  value       = aws_db_instance.airflow.endpoint
}

output "rds_port" {
  description = "RDS port for Airflow metadata database"
  value       = aws_db_instance.airflow.port
}

output "rds_database_name" {
  description = "RDS database name"
  value       = aws_db_instance.airflow.db_name
}

output "rds_username" {
  description = "RDS username"
  value       = aws_db_instance.airflow.username
  sensitive   = true
}

# Redis Outputs
output "redis_endpoint" {
  description = "Redis endpoint for Celery backend"
  value       = aws_elasticache_cluster.airflow.cluster_address
}

output "redis_port" {
  description = "Redis port"
  value       = aws_elasticache_cluster.airflow.port
}

# S3 Bucket Outputs
output "s3_bucket_name" {
  description = "S3 bucket name for Airflow DAGs and logs"
  value       = aws_s3_bucket.airflow_bucket.id
}

output "s3_bucket_arn" {
  description = "S3 bucket ARN for Airflow DAGs and logs"
  value       = aws_s3_bucket.airflow_bucket.arn
}

# KMS Key Outputs
output "kms_key_id" {
  description = "KMS key ID for Airflow encryption"
  value       = aws_kms_key.airflow.key_id
}

output "kms_key_arn" {
  description = "KMS key ARN for Airflow encryption"
  value       = aws_kms_key.airflow.arn
}

# IAM Role Outputs
output "execution_role_arn" {
  description = "IAM role ARN for Airflow execution"
  value       = aws_iam_role.airflow_execution_role.arn
}

output "execution_role_name" {
  description = "IAM role name for Airflow execution"
  value       = aws_iam_role.airflow_execution_role.name
}

# Kubernetes Outputs
output "kubernetes_namespace" {
  description = "Kubernetes namespace for Airflow"
  value       = kubernetes_namespace.airflow.metadata[0].name
}

output "service_account_name" {
  description = "Kubernetes service account name for Airflow"
  value       = kubernetes_service_account.airflow.metadata[0].name
}

output "helm_release_name" {
  description = "Helm release name for Airflow"
  value       = helm_release.airflow.name
}

output "helm_release_status" {
  description = "Helm release status for Airflow"
  value       = helm_release.airflow.status
}

# Load Balancer Outputs
output "webserver_load_balancer_hostname" {
  description = "Load balancer hostname for Airflow webserver"
  value       = var.enable_load_balancer ? kubernetes_service.airflow_webserver_lb[0].status[0].load_balancer[0].ingress[0].hostname : null
}

output "webserver_load_balancer_ip" {
  description = "Load balancer IP for Airflow webserver"
  value       = var.enable_load_balancer ? kubernetes_service.airflow_webserver_lb[0].status[0].load_balancer[0].ingress[0].ip : null
}

# Connection Information
output "connection_info" {
  description = "Connection information for Airflow services"
  value = {
    webserver_url = var.enable_load_balancer ? (
      kubernetes_service.airflow_webserver_lb[0].status[0].load_balancer[0].ingress[0].hostname != null ?
      "http://${kubernetes_service.airflow_webserver_lb[0].status[0].load_balancer[0].ingress[0].hostname}" :
      "http://${kubernetes_service.airflow_webserver_lb[0].status[0].load_balancer[0].ingress[0].ip}"
    ) : "kubectl port-forward svc/airflow-webserver 8080:8080 -n ${kubernetes_namespace.airflow.metadata[0].name}"

    flower_url = var.enable_flower && var.airflow_executor == "CeleryExecutor" ?
      "kubectl port-forward svc/airflow-flower ${var.flower_port}:${var.flower_port} -n ${kubernetes_namespace.airflow.metadata[0].name}" :
      "N/A - Flower not enabled"

    database_connection_string = "postgresql://${aws_db_instance.airflow.username}@${aws_db_instance.airflow.endpoint}:${aws_db_instance.airflow.port}/${aws_db_instance.airflow.db_name}"

    redis_connection_string = var.airflow_executor == "CeleryExecutor" ?
      "redis://${aws_elasticache_cluster.airflow.cluster_address}:${aws_elasticache_cluster.airflow.port}/0" :
      "N/A - Redis not used with ${var.airflow_executor}"
  }
}

# Configuration Summary
output "airflow_configuration" {
  description = "Airflow deployment configuration summary"
  value = {
    executor                = var.airflow_executor
    webserver_replicas     = var.webserver_replicas
    scheduler_replicas     = var.scheduler_replicas
    worker_replicas        = var.airflow_executor == "CeleryExecutor" ? var.worker_replicas : "N/A"
    max_worker_replicas    = var.airflow_executor == "CeleryExecutor" ? var.worker_max_replicas : "N/A"
    flower_enabled         = var.enable_flower && var.airflow_executor == "CeleryExecutor"
    prometheus_monitoring  = var.enable_prometheus_monitoring
    load_balancer_enabled  = var.enable_load_balancer
    namespace              = kubernetes_namespace.airflow.metadata[0].name
  }
}

# Resource Information
output "resource_details" {
  description = "AWS resource details for Airflow deployment"
  value = {
    rds_instance = {
      identifier     = aws_db_instance.airflow.identifier
      instance_class = aws_db_instance.airflow.instance_class
      engine_version = aws_db_instance.airflow.engine_version
      storage_gb     = aws_db_instance.airflow.allocated_storage
      multi_az       = aws_db_instance.airflow.multi_az
      encrypted      = aws_db_instance.airflow.storage_encrypted
    }

    redis_cluster = {
      cluster_id   = aws_elasticache_cluster.airflow.cluster_id
      node_type    = aws_elasticache_cluster.airflow.node_type
      num_nodes    = aws_elasticache_cluster.airflow.num_cache_nodes
      engine       = aws_elasticache_cluster.airflow.engine
      port         = aws_elasticache_cluster.airflow.port
      encrypted    = aws_elasticache_cluster.airflow.at_rest_encryption_enabled
    }

    s3_bucket = {
      name       = aws_s3_bucket.airflow_bucket.id
      region     = aws_s3_bucket.airflow_bucket.region
      versioning = aws_s3_bucket_versioning.airflow_bucket_versioning.versioning_configuration[0].status
      encrypted  = true
    }
  }
}

# Cost Optimization Features
output "cost_optimization" {
  description = "Cost optimization features enabled"
  value = {
    rds_performance_insights = aws_db_instance.airflow.performance_insights_enabled
    rds_backup_retention     = aws_db_instance.airflow.backup_retention_period
    auto_scaling_enabled     = var.airflow_executor == "CeleryExecutor"
    spot_instances_ready     = "Configure in EKS node groups"
    storage_lifecycle        = "S3 intelligent tiering enabled"
  }
}

# Security Features
output "security_features" {
  description = "Security features enabled"
  value = {
    database_encryption      = aws_db_instance.airflow.storage_encrypted
    redis_encryption_rest    = aws_elasticache_cluster.airflow.at_rest_encryption_enabled
    redis_encryption_transit = aws_elasticache_cluster.airflow.transit_encryption_enabled
    s3_encryption           = "SSE-KMS enabled"
    kms_key_rotation        = "Automatic rotation enabled"
    iam_roles_for_sa        = "IRSA enabled for Kubernetes workloads"
    network_isolation       = "Private subnets only"
  }
}

# Monitoring Integration
output "monitoring_integration" {
  description = "Monitoring and observability integration points"
  value = {
    prometheus_metrics    = var.enable_prometheus_monitoring
    cloudwatch_logs      = "Enabled for RDS and ElastiCache"
    airflow_metrics      = "StatsD metrics collection enabled"
    flower_monitoring    = var.enable_flower && var.airflow_executor == "CeleryExecutor"
    log_aggregation      = "Kubernetes logs to CloudWatch"
    performance_insights = aws_db_instance.airflow.performance_insights_enabled
  }
}

# EMR Integration
output "emr_integration" {
  description = "EMR integration configuration"
  value = {
    emr_cluster_id          = var.emr_cluster_id
    emr_service_role        = var.emr_service_role_arn
    emr_instance_profile    = var.emr_instance_profile_arn
    iam_permissions_granted = "Yes - Airflow can manage EMR jobs"
    spark_job_submission    = "Enabled via EMR operators"
  }
}
