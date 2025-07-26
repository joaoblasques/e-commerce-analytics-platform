# EMR Cluster Module Outputs

output "cluster_id" {
  description = "ID of the EMR cluster"
  value       = aws_emr_cluster.cluster.id
}

output "cluster_name" {
  description = "Name of the EMR cluster"
  value       = aws_emr_cluster.cluster.name
}

output "cluster_arn" {
  description = "ARN of the EMR cluster"
  value       = aws_emr_cluster.cluster.arn
}

output "master_public_dns" {
  description = "Master node public DNS name"
  value       = aws_emr_cluster.cluster.master_public_dns
}

output "cluster_state" {
  description = "State of the EMR cluster"
  value       = aws_emr_cluster.cluster.cluster_state
}

output "log_uri" {
  description = "S3 bucket URI for cluster logs"
  value       = aws_emr_cluster.cluster.log_uri
}

# IAM Roles
output "service_role_arn" {
  description = "ARN of the EMR service role"
  value       = aws_iam_role.emr_service_role.arn
}

output "ec2_instance_role_arn" {
  description = "ARN of the EMR EC2 instance role"
  value       = aws_iam_role.emr_ec2_instance_role.arn
}

output "instance_profile_arn" {
  description = "ARN of the EMR EC2 instance profile"
  value       = aws_iam_instance_profile.emr_ec2_instance_profile.arn
}

output "autoscaling_role_arn" {
  description = "ARN of the EMR autoscaling role"
  value       = var.auto_scaling_enabled ? aws_iam_role.emr_autoscaling_role[0].arn : null
}

# Security Configuration
output "security_configuration_name" {
  description = "Name of the EMR security configuration"
  value       = var.encryption_at_rest_enabled || var.encryption_in_transit_enabled ? aws_emr_security_configuration.emr_security_config[0].name : null
}

# Task Instance Group
output "task_instance_group_id" {
  description = "ID of the task instance group"
  value       = var.spot_instance_enabled ? aws_emr_instance_group.task_group[0].id : null
}

# Monitoring
output "memory_utilization_alarm_arn" {
  description = "ARN of the memory utilization CloudWatch alarm"
  value       = var.enable_cloudwatch_monitoring ? aws_cloudwatch_metric_alarm.cluster_memory_utilization[0].arn : null
}

output "apps_failed_alarm_arn" {
  description = "ARN of the failed applications CloudWatch alarm"
  value       = var.enable_cloudwatch_monitoring ? aws_cloudwatch_metric_alarm.cluster_apps_failed[0].arn : null
}

# Configuration Details
output "cluster_configuration" {
  description = "Cluster configuration summary"
  value = {
    applications     = var.applications
    release_label    = var.release_label
    master_instance  = var.master_instance_type
    core_instances   = "${var.core_instance_count}x ${var.core_instance_type}"
    task_instances   = var.spot_instance_enabled ? "${var.task_instance_count}x ${var.task_instance_type} (spot)" : "disabled"
    auto_scaling     = var.auto_scaling_enabled
    spot_enabled     = var.spot_instance_enabled
    encryption       = var.encryption_at_rest_enabled || var.encryption_in_transit_enabled
  }
}

# Cost Information
output "cost_optimization_features" {
  description = "Enabled cost optimization features"
  value = {
    spot_instances         = var.spot_instance_enabled
    spot_bid_percentage    = var.spot_instance_enabled ? var.spot_bid_percentage : null
    auto_scaling           = var.auto_scaling_enabled
    auto_termination       = var.idle_timeout
    scale_down_behavior    = var.scale_down_behavior
    terminate_unhealthy    = var.terminate_unhealthy_nodes
  }
}

# Connection Information
output "connection_info" {
  description = "Connection information for accessing the cluster"
  value = {
    master_dns           = aws_emr_cluster.cluster.master_public_dns
    spark_ui_url        = "http://${aws_emr_cluster.cluster.master_public_dns}:8088"
    spark_history_url   = "http://${aws_emr_cluster.cluster.master_public_dns}:18080"
    hadoop_ui_url       = "http://${aws_emr_cluster.cluster.master_public_dns}:8088"
    yarn_ui_url         = "http://${aws_emr_cluster.cluster.master_public_dns}:8088"
    zeppelin_url        = "http://${aws_emr_cluster.cluster.master_public_dns}:8890"
    jupyter_url         = "http://${aws_emr_cluster.cluster.master_public_dns}:9443"
    livy_url           = "http://${aws_emr_cluster.cluster.master_public_dns}:8998"
  }
}

# Logs and Monitoring
output "monitoring_info" {
  description = "Monitoring and logging information"
  value = {
    log_bucket           = var.log_bucket
    emr_logs_path       = "s3://${var.log_bucket}/emr-logs/"
    spark_event_logs    = "s3://${var.log_bucket}/spark-event-logs/"
    yarn_logs_path      = "s3://${var.log_bucket}/yarn-logs/"
    cloudwatch_enabled  = var.enable_cloudwatch_monitoring
    debugging_enabled   = var.enable_debugging
  }
}
