# RDS Module Outputs

output "db_instance_id" {
  description = "The RDS instance ID"
  value       = aws_db_instance.main.id
}

output "db_instance_arn" {
  description = "The ARN of the RDS instance"
  value       = aws_db_instance.main.arn
}

output "db_instance_endpoint" {
  description = "The RDS instance endpoint"
  value       = aws_db_instance.main.endpoint
}

output "db_instance_hosted_zone_id" {
  description = "The canonical hosted zone ID of the DB instance (to be used in a Route 53 Alias record)"
  value       = aws_db_instance.main.hosted_zone_id
}

output "db_instance_port" {
  description = "The RDS instance port"
  value       = aws_db_instance.main.port
}

output "db_instance_name" {
  description = "The database name"
  value       = aws_db_instance.main.db_name
}

output "db_instance_username" {
  description = "The master username for the database"
  value       = aws_db_instance.main.username
  sensitive   = true
}

output "db_instance_password" {
  description = "The database password (this password may be old, because Terraform doesn't track it after initial creation)"
  value       = random_password.master.result
  sensitive   = true
}

output "db_instance_address" {
  description = "The address of the RDS instance"
  value       = aws_db_instance.main.address
}

output "db_instance_availability_zone" {
  description = "The availability zone of the RDS instance"
  value       = aws_db_instance.main.availability_zone
}

output "db_instance_status" {
  description = "The RDS instance status"
  value       = aws_db_instance.main.status
}

# Read Replica Outputs
output "read_replica_id" {
  description = "The RDS read replica instance ID"
  value       = var.create_read_replica ? aws_db_instance.read_replica[0].id : null
}

output "read_replica_arn" {
  description = "The ARN of the RDS read replica instance"
  value       = var.create_read_replica ? aws_db_instance.read_replica[0].arn : null
}

output "read_replica_endpoint" {
  description = "The RDS read replica instance endpoint"
  value       = var.create_read_replica ? aws_db_instance.read_replica[0].endpoint : null
}

output "read_replica_address" {
  description = "The address of the RDS read replica instance"
  value       = var.create_read_replica ? aws_db_instance.read_replica[0].address : null
}

# Parameter and Option Groups
output "parameter_group_id" {
  description = "The DB parameter group name"
  value       = aws_db_parameter_group.main.id
}

output "parameter_group_arn" {
  description = "The ARN of the DB parameter group"
  value       = aws_db_parameter_group.main.arn
}

output "option_group_id" {
  description = "The DB option group name"
  value       = aws_db_option_group.main.id
}

output "option_group_arn" {
  description = "The ARN of the DB option group"
  value       = aws_db_option_group.main.arn
}

# KMS Key
output "kms_key_id" {
  description = "The globally unique identifier for the key"
  value       = aws_kms_key.rds.key_id
}

output "kms_key_arn" {
  description = "The Amazon Resource Name (ARN) of the key"
  value       = aws_kms_key.rds.arn
}

# Secrets Manager
output "db_password_secret_arn" {
  description = "The ARN of the Secrets Manager secret storing the database password"
  value       = aws_secretsmanager_secret.db_password.arn
}

output "db_password_secret_name" {
  description = "The name of the Secrets Manager secret storing the database password"
  value       = aws_secretsmanager_secret.db_password.name
}

# Enhanced Monitoring
output "enhanced_monitoring_iam_role_arn" {
  description = "The Amazon Resource Name (ARN) specifying the monitoring role"
  value       = var.monitoring_interval > 0 ? aws_iam_role.rds_enhanced_monitoring[0].arn : null
}

# CloudWatch Alarms
output "cpu_alarm_id" {
  description = "The ID of the CPU utilization alarm"
  value       = aws_cloudwatch_metric_alarm.database_cpu.id
}

output "disk_queue_alarm_id" {
  description = "The ID of the disk queue depth alarm"
  value       = aws_cloudwatch_metric_alarm.database_disk_queue.id
}

output "storage_alarm_id" {
  description = "The ID of the free storage space alarm"
  value       = aws_cloudwatch_metric_alarm.database_disk_free.id
}

output "memory_alarm_id" {
  description = "The ID of the freeable memory alarm"
  value       = aws_cloudwatch_metric_alarm.database_memory_freeable.id
}

# Connection Information for Applications
output "connection_string" {
  description = "PostgreSQL connection string"
  value       = "postgresql://${aws_db_instance.main.username}:${random_password.master.result}@${aws_db_instance.main.endpoint}:${aws_db_instance.main.port}/${aws_db_instance.main.db_name}"
  sensitive   = true
}

output "jdbc_connection_string" {
  description = "JDBC connection string"
  value       = "jdbc:postgresql://${aws_db_instance.main.endpoint}:${aws_db_instance.main.port}/${aws_db_instance.main.db_name}"
}

# Connection Information for Read Replica
output "read_replica_connection_string" {
  description = "PostgreSQL connection string for read replica"
  value       = var.create_read_replica ? "postgresql://${aws_db_instance.main.username}:${random_password.master.result}@${aws_db_instance.read_replica[0].endpoint}:${aws_db_instance.read_replica[0].port}/${aws_db_instance.main.db_name}" : null
  sensitive   = true
}

output "read_replica_jdbc_connection_string" {
  description = "JDBC connection string for read replica"
  value       = var.create_read_replica ? "jdbc:postgresql://${aws_db_instance.read_replica[0].endpoint}:${aws_db_instance.read_replica[0].port}/${aws_db_instance.main.db_name}" : null
}
