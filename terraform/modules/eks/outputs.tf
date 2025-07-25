# EKS Module Outputs

output "cluster_id" {
  description = "The ID of the EKS cluster"
  value       = aws_eks_cluster.main.id
}

output "cluster_arn" {
  description = "The ARN of the EKS cluster"
  value       = aws_eks_cluster.main.arn
}

output "cluster_endpoint" {
  description = "Endpoint for EKS control plane"
  value       = aws_eks_cluster.main.endpoint
}

output "cluster_version" {
  description = "The Kubernetes version for the EKS cluster"
  value       = aws_eks_cluster.main.version
}

output "cluster_platform_version" {
  description = "Platform version for the EKS cluster"
  value       = aws_eks_cluster.main.platform_version
}

output "cluster_status" {
  description = "Status of the EKS cluster. One of `CREATING`, `ACTIVE`, `DELETING`, `FAILED`"
  value       = aws_eks_cluster.main.status
}

# Cluster Security
output "cluster_security_group_id" {
  description = "Security group ID attached to the EKS cluster"
  value       = aws_eks_cluster.main.vpc_config[0].cluster_security_group_id
}

output "cluster_certificate_authority_data" {
  description = "Base64 encoded certificate data required to communicate with the cluster"
  value       = aws_eks_cluster.main.certificate_authority[0].data
}

# OIDC Provider
output "cluster_oidc_issuer_url" {
  description = "The URL on the EKS cluster for the OpenID Connect identity provider"
  value       = aws_eks_cluster.main.identity[0].oidc[0].issuer
}

output "oidc_provider_arn" {
  description = "The ARN of the OIDC Provider for service accounts"
  value       = aws_iam_openid_connect_provider.eks.arn
}

# Node Groups
output "primary_node_group_arn" {
  description = "ARN of the primary EKS node group"
  value       = aws_eks_node_group.primary.arn
}

output "primary_node_group_status" {
  description = "Status of the primary EKS node group"
  value       = aws_eks_node_group.primary.status
}

output "spot_node_group_arn" {
  description = "ARN of the spot EKS node group"
  value       = var.enable_spot_instances ? aws_eks_node_group.spot[0].arn : null
}

output "spot_node_group_status" {
  description = "Status of the spot EKS node group"
  value       = var.enable_spot_instances ? aws_eks_node_group.spot[0].status : null
}

# IAM Roles
output "cluster_iam_role_arn" {
  description = "IAM role ARN of the EKS cluster"
  value       = aws_iam_role.cluster.arn
}

output "node_group_iam_role_arn" {
  description = "IAM role ARN of the EKS node group"
  value       = aws_iam_role.node_group.arn
}

# KMS
output "kms_key_id" {
  description = "KMS key ID used for EKS cluster encryption"
  value       = aws_kms_key.eks.key_id
}

output "kms_key_arn" {
  description = "KMS key ARN used for EKS cluster encryption"
  value       = aws_kms_key.eks.arn
}

# CloudWatch
output "cloudwatch_log_group_name" {
  description = "Name of the CloudWatch log group for EKS cluster logs"
  value       = aws_cloudwatch_log_group.eks.name
}

output "cloudwatch_log_group_arn" {
  description = "ARN of the CloudWatch log group for EKS cluster logs"
  value       = aws_cloudwatch_log_group.eks.arn
}

# Node Group Additional Outputs
output "primary_node_group_asg_name" {
  description = "Name of the Auto Scaling Group associated with the primary EKS node group"
  value       = aws_eks_node_group.primary.resources[0].autoscaling_groups[0].name
}

output "spot_node_group_asg_name" {
  description = "Name of the Auto Scaling Group associated with the spot EKS node group"
  value       = var.enable_spot_instances ? aws_eks_node_group.spot[0].resources[0].autoscaling_groups[0].name : null
}

# Kubeconfig helper
output "kubeconfig_update_command" {
  description = "Command to update kubeconfig for this cluster"
  value       = "aws eks update-kubeconfig --region ${data.aws_region.current.name} --name ${aws_eks_cluster.main.name}"
}

# Data source for current region
data "aws_region" "current" {}
