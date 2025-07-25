# Security Groups Module Outputs

output "alb_security_group_id" {
  description = "ID of the Application Load Balancer security group"
  value       = aws_security_group.alb.id
}

output "eks_cluster_security_group_id" {
  description = "ID of the EKS cluster security group"
  value       = aws_security_group.eks_cluster.id
}

output "eks_worker_nodes_security_group_id" {
  description = "ID of the EKS worker nodes security group"
  value       = aws_security_group.eks_worker_nodes.id
}

output "rds_postgres_security_group_id" {
  description = "ID of the RDS PostgreSQL security group"
  value       = aws_security_group.rds_postgres.id
}

output "elasticache_redis_security_group_id" {
  description = "ID of the ElastiCache Redis security group"
  value       = aws_security_group.elasticache_redis.id
}

output "msk_security_group_id" {
  description = "ID of the MSK (Managed Kafka) security group"
  value       = aws_security_group.msk.id
}

output "emr_security_group_id" {
  description = "ID of the EMR security group"
  value       = aws_security_group.emr.id
}

output "bastion_security_group_id" {
  description = "ID of the bastion host security group (if enabled)"
  value       = var.enable_bastion ? aws_security_group.bastion[0].id : null
}

output "monitoring_security_group_id" {
  description = "ID of the monitoring services security group"
  value       = aws_security_group.monitoring.id
}

# Security Group ARNs
output "alb_security_group_arn" {
  description = "ARN of the Application Load Balancer security group"
  value       = aws_security_group.alb.arn
}

output "eks_cluster_security_group_arn" {
  description = "ARN of the EKS cluster security group"
  value       = aws_security_group.eks_cluster.arn
}

output "eks_worker_nodes_security_group_arn" {
  description = "ARN of the EKS worker nodes security group"
  value       = aws_security_group.eks_worker_nodes.arn
}

output "rds_postgres_security_group_arn" {
  description = "ARN of the RDS PostgreSQL security group"
  value       = aws_security_group.rds_postgres.arn
}

output "elasticache_redis_security_group_arn" {
  description = "ARN of the ElastiCache Redis security group"
  value       = aws_security_group.elasticache_redis.arn
}

output "msk_security_group_arn" {
  description = "ARN of the MSK (Managed Kafka) security group"
  value       = aws_security_group.msk.arn
}

output "emr_security_group_arn" {
  description = "ARN of the EMR security group"
  value       = aws_security_group.emr.arn
}

output "monitoring_security_group_arn" {
  description = "ARN of the monitoring services security group"
  value       = aws_security_group.monitoring.arn
}
